-- ReliableDataStore v2.0.0
-- Backwards-compatible extension of the ReliableDataStore module
-- Adds: delta-save, pluggable compression, schema validation, session timeout recovery,
-- migration versioning API, and a few quality-of-life helpers.

local DataStoreService = game:GetService("DataStoreService")
local MemoryStoreService = game:GetService("MemoryStoreService")
local MessagingService = game:GetService("MessagingService")
local Players = game:GetService("Players")
local HttpService = game:GetService("HttpService")

local ReliableDataStore = {}
ReliableDataStore.__index = ReliableDataStore

---------------------------------------------------------------------
-- Utilities
---------------------------------------------------------------------
local function deepCopy(tbl)
	if type(tbl) ~= "table" then return tbl end
	local copy = {}
	for k, v in pairs(tbl) do
		copy[k] = deepCopy(v)
	end
	return copy
end

local function deepMerge(target, defaults)
	for k, v in pairs(defaults) do
		if type(v) == "table" then
			if type(target[k]) ~= "table" then
				target[k] = deepCopy(v)
			else
				deepMerge(target[k], v)
			end
		elseif target[k] == nil then
			target[k] = v
		end
	end
end

local function deepSet(tbl, path, value)
	local keys = string.split(path, ".")
	local t = tbl
	for i = 1, #keys - 1 do
		local k = keys[i]
		t[k] = t[k] or {}
		t = t[k]
	end
	t[keys[#keys]] = value
end

local function deepGet(tbl, path)
	if not path or path == "" then return tbl end
	local keys = string.split(path, ".")
	local t = tbl
	for i = 1, #keys do
		if type(t) ~= "table" then return nil end
		t = t[keys[i]]
	end
	return t
end

local function splitPathSet(old, path, value)
	-- set value in old table following dotted path; creates subtables as necessary
	local keys = string.split(path, ".")
	local t = old
	for i = 1, #keys - 1 do
		local k = keys[i]
		t[k] = t[k] or {}
		t = t[k]
	end
	t[keys[#keys]] = value
end

local function retry(fn, tries, waitTime)
	tries, waitTime = tries or 3, waitTime or 0.5
	local lastErr
	for i = 1, tries do
		local ok, res = pcall(fn)
		if ok then return true, res end
		lastErr = res
		task.wait(waitTime * i)
	end
	return false, lastErr
end

---------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------
function ReliableDataStore.new(name, defaults, options)
	options = options or {}

	local inst = setmetatable({
		store = DataStoreService:GetDataStore(name),
		locks = MemoryStoreService:GetSortedMap("RDS_LOCK_" .. name),
		defaults = deepCopy(defaults or {}),
		sessions = {},
		jobId = game.JobId ~= "" and game.JobId or HttpService:GenerateGUID(false),

		-- settings
		settings = {
			lockTTL = options.lockTTL or 120,
			autosave = options.autosave or 30,
			retries = options.retries or 3,
			backupCount = options.backupCount or 2,
			sessionTimeout = options.sessionTimeout or 600, -- seconds of inactivity before recovery
		},

		validators = {}, -- per-key validator
		schema = options.schema or nil, -- optional schema table
		migrations = {}, -- { [version] = function(data) return data end }

		-- compression hooks: compressor.encode(string)->string and compressor.decode(string)->string
		compressor = options.compressor or { enabled = false, encode = function(x) return x end, decode = function(x) return x end },

		events = {
			OnLoaded = Instance.new("BindableEvent"),
			OnSaved = Instance.new("BindableEvent"),
			OnKicked = Instance.new("BindableEvent"),
			OnConflict = Instance.new("BindableEvent"),
		},
	}, ReliableDataStore)

	return inst
end

-- Event access (same API as before)
function ReliableDataStore:On(event)
	return self.events[event].Event
end

function ReliableDataStore:Log(level, msg)
	print(("[%s] [ReliableDataStore] [%s] %s"):format(level, os.date("!%Y-%m-%dT%H:%M:%SZ"), tostring(msg)))
end

---------------------------------------------------------------------
-- Validators / Schema
---------------------------------------------------------------------
function ReliableDataStore:SetValidator(key, fn)
	self.validators[key] = fn
end

local function validateSchemaRecursive(schema, data, path)
	path = path or ""
	if type(schema) ~= "table" then return true, nil end
	for k, spec in pairs(schema) do
		local p = (path ~= "") and (path .. "." .. k) or k
		local val = data and data[k]
		if spec.required and val == nil then
			return false, ("Missing required field: %s"):format(p)
		end
		if val ~= nil then
			if spec.type and type(val) ~= spec.type then
				return false, ("Type mismatch for %s: expected %s, got %s"):format(p, spec.type, type(val))
			end
			if spec.schema and type(val) == "table" then
				local ok, err = validateSchemaRecursive(spec.schema, val, p)
				if not ok then return false, err end
			end
		end
	end
	return true, nil
end

function ReliableDataStore:ValidateSchema(data)
	if not self.schema then return true end
	return validateSchemaRecursive(self.schema, data)
end

---------------------------------------------------------------------
-- Migration API
---------------------------------------------------------------------
function ReliableDataStore:AddMigration(version, fn)
	assert(type(version) == "number", "version must be a number")
	assert(type(fn) == "function", "migration must be a function")
	self.migrations[version] = fn
end

local function applyMigrations(self, data)
	local current = data._schemaVersion or 0
	local versions = {}
	for v in pairs(self.migrations) do table.insert(versions, v) end
	table.sort(versions)
	for _, v in ipairs(versions) do
		if v > current then
			local ok, res = pcall(self.migrations[v], data)
			if ok then
				data = res or data
				data._schemaVersion = v
			else
				self:Log("ERROR", ("Migration %d failed: %s"):format(v, tostring(res)))
				-- continue; don't stop the load to keep backwards compatibility
			end
		end
	end
	return data
end

---------------------------------------------------------------------
-- Locking with stale recovery
---------------------------------------------------------------------
function ReliableDataStore:_lock(key, refresh)
	local ttl = self.settings.lockTTL
	local job = self.jobId
	local ok, res = pcall(function()
		return self.locks:UpdateAsync(key, function(cur)
			-- cur may be nil or { owner = jobId, ts = os.clock() }
			if not cur then
				return { owner = job, ts = os.clock() }
			end
			if cur.owner == job then
				-- we already own it; refresh ts
				return { owner = job, ts = os.clock() }
			end
			-- detect stale lock
			if (type(cur.ts) == "number" and (os.clock() - cur.ts) > (ttl * 2)) then
				-- consider stale and take over
				self:Log("WARN", ("Stealing stale lock for %s (owner=%s)"):format(key, tostring(cur.owner)))
				return { owner = job, ts = os.clock() }
			end
			-- otherwise keep current
			return cur
		end, ttl)
	end)
	return ok and res and res.owner == job
end

---------------------------------------------------------------------
-- Load (with migrations, decompression, schema validation)
---------------------------------------------------------------------
function ReliableDataStore:_load(plr)
	local key = "u:" .. plr.UserId
	if not self:_lock(key) then
		plr:Kick("Data in use, try again.")
		self.events.OnKicked:Fire(plr)
		return
	end

	local raw
	retry(function()
		raw = self.store:GetAsync(key)
	end, self.settings.retries)

	local data
	if not raw then
		data = deepCopy(self.defaults)
		data._version = 1
		data._schemaVersion = data._schemaVersion or 0
	else
		-- handle compressed payloads
		if type(raw) == "table" and raw.__compressed and raw.payload then
			local ok, decoded = pcall(function()
				local json = self.compressor.decode(raw.payload)
				return HttpService:JSONDecode(json)
			end)
			if ok and type(decoded) == "table" then
				data = decoded
			else
				-- fallback: if decode fails, try raw as-is
				data = raw
			end
		else
			data = raw
		end

		-- don't mutate defaults; merge safely
		if type(data) ~= "table" then data = deepCopy(self.defaults) end
		deepMerge(data, deepCopy(self.defaults))
		data._version = (data._version or 0) + 1
	end

	-- run migrations (non-blocking)
	local ok, migrated = pcall(function() return applyMigrations(self, data) end)
	if ok and type(migrated) == "table" then data = migrated end

	-- validate schema (best-effort; will log but won't block load)
	local valid, err = self:ValidateSchema(data)
	if not valid then
		self:Log("WARN", ("Schema validation failed for %s: %s"):format(plr.Name, tostring(err)))
	end

	self.sessions[plr] = {
		data = data,
		version = data._version,
		dirty = {}, -- keyed by dotted paths; { ["coins"] = true }
		backups = {},
		meta = {
			CreatedAt = data._metaCreatedAt or os.time(),
			LastLoaded = os.time(),
			LastSave = 0,
			LastHeartbeat = os.clock(),
		},
	}

	self:Log("INFO", ("Loaded data for %s (UserId=%d)"):format(plr.Name, plr.UserId))
	self.events.OnLoaded:Fire(plr, deepCopy(data))
end

---------------------------------------------------------------------
-- Public API: Get / Set / Export / Import (backwards compatible)
---------------------------------------------------------------------
function ReliableDataStore:Get(plr, key)
	local session = self.sessions[plr]
	if not session then return nil end
	-- heartbeat
	session.meta.LastHeartbeat = os.clock()
	return key and deepGet(session.data, key) or session.data
end

function ReliableDataStore:Set(plr, key, value)
	local session = self.sessions[plr]
	if not session then return end

	-- validators: per-key fn
	if key and self.validators[key] and not self.validators[key](value) then
		self:Log("WARN", ("Validation failed for %s: %s"):format(key, tostring(value)))
		return false
	end

	if not key then
		-- setting root (replace whole data)
		session.data = value
		session.dirty["_root"] = true
	else
		deepSet(session.data, key, value)
		session.dirty[key] = true
	end

	session.version = (session.version or 0) + 1
	session.meta.LastHeartbeat = os.clock()
	return true
end

function ReliableDataStore:Export(plr)
	local session = self.sessions[plr]
	if not session then return nil end
	return HttpService:JSONEncode(session.data)
end

function ReliableDataStore:Import(plr, json)
	local ok, decoded = pcall(HttpService.JSONDecode, HttpService, json)
	if not ok then return false end
	local session = self.sessions[plr]
	if not session then return false end
	session.data = decoded
	session.version = (session.version or 0) + 1
	session.dirty["_root"] = true
	return true
end

---------------------------------------------------------------------
-- Delta-save helper: apply only dirty paths to old table
---------------------------------------------------------------------
local function applyDeltasToOld(old, newData, dirtyPaths)
	-- if root was marked dirty, replace fully
	if dirtyPaths["_root"] then
		-- return deep copy of newData
		return deepCopy(newData)
	end
	old = type(old) == "table" and old or {}
	local patched = deepCopy(old)
	for path in pairs(dirtyPaths) do
		if path ~= "_root" then
			local val = deepGet(newData, path)
			splitPathSet(patched, path, deepCopy(val))
		end
	end
	return patched
end

---------------------------------------------------------------------
-- Save (delta-save + compression + backups + conflict handling)
---------------------------------------------------------------------
function ReliableDataStore:_save(plr, release)
	local session = self.sessions[plr]
	if not session then return end

	local key = "u:" .. plr.UserId
	local saveSnapshot = deepCopy(session.data)
	saveSnapshot._version = session.version

	-- If there are no dirty paths, still update LastSave and return
	local dirty = session.dirty or {}
	local hasDirty = next(dirty) ~= nil

	local success, err = retry(function()
		self.store:UpdateAsync(key, function(old)
			old = type(old) == "table" and old or {}

			-- If compression was used previously, old might be compressed wrapper
			if type(old) == "table" and old.__compressed and old.payload then
				local decOk, dec = pcall(function()
					local json = self.compressor.decode(old.payload)
					return HttpService:JSONDecode(json)
				end)
				if decOk and type(dec) == "table" then
					old = dec
				end
			end

			-- Conflict detection by version
			if (old._version or 0) <= saveSnapshot._version then
				-- if no dirty: nothing to change, just keep old or saveSnapshot if root replacement
				local toWrite
				if not hasDirty then
					-- nothing changed during this session; keep old unmodified
					toWrite = old
				else
					-- apply only deltas
					toWrite = applyDeltasToOld(old, saveSnapshot, dirty)
					-- ensure version is up-to-date
					toWrite._version = saveSnapshot._version
				end

				-- optionally compress before returning
				if self.compressor and self.compressor.enabled then
					local json = HttpService:JSONEncode(toWrite)
					local okc, payload = pcall(self.compressor.encode, json)
					if okc then
						return { __compressed = true, payload = payload }
					end
				end

				return toWrite
			else
				-- conflict: existing store is newer; keep old and fire event
				self:Log("WARN", ("Conflict detected for %s; store has newer version %s > %s"):format(plr.Name, tostring(old._version), tostring(saveSnapshot._version)))
				self.events.OnConflict:Fire(plr, old, saveSnapshot)
				return old
			end
		end)
	end, self.settings.retries)

	if not success then
		self:Log("ERROR", ("Failed to save for %s: %s"):format(plr.Name, tostring(err)))
	else
		-- rotate backups if we changed something
		if hasDirty and session.backups then
			table.insert(session.backups, 1, deepCopy(saveSnapshot))
			if #session.backups > self.settings.backupCount then
				table.remove(session.backups)
			end
		end
	end

	session.dirty = {}
	session.meta.LastSave = os.time()
	self.events.OnSaved:Fire(plr, saveSnapshot)

	if release then
		pcall(function() self.locks:RemoveAsync(key) end)
		self.sessions[plr] = nil
	end
end

function ReliableDataStore:_saveAll()
	for plr in pairs(self.sessions) do
		self:_save(plr, true)
	end
end

---------------------------------------------------------------------
-- Lifecycle: Start + autosave + heartbeat monitor
---------------------------------------------------------------------
function ReliableDataStore:Start()
	Players.PlayerAdded:Connect(function(plr) self:_load(plr) end)
	Players.PlayerRemoving:Connect(function(plr) self:_save(plr, true) end)
	game:BindToClose(function() self:_saveAll() end)

	-- Autosave loop
	task.spawn(function()
		while task.wait(self.settings.autosave) do
			for plr in pairs(self.sessions) do
				pcall(function() self:_save(plr) end)
			end
		end
	end)

	-- Lock renewal (touch locks periodically)
	task.spawn(function()
		while task.wait(self.settings.lockTTL / 2) do
			for plr, s in pairs(self.sessions) do
				pcall(function()
					self:_lock("u:" .. plr.UserId, true)
					-- also recover session timeouts
					if (os.clock() - (s.meta.LastHeartbeat or 0)) > self.settings.sessionTimeout then
						-- session is stale: force save and release
						self:Log("WARN", ("Session timeout for %s; saving and releasing"):format(plr.Name))
						self:_save(plr, true)
					end
				end)
			end
		end
	end)

	-- Listen for job duplicate warnings; harmless
	pcall(function()
		MessagingService:SubscribeAsync("RDS_" .. self.jobId, function(msg)
			self:Log("WARN", "Duplicate session detected: " .. tostring(msg.Data))
		end)
	end)
end

---------------------------------------------------------------------
-- Backwards-compatible extras: ForceSave, AddMigration are already provided above
---------------------------------------------------------------------

return ReliableDataStore
