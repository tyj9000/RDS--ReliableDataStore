local DataStoreService = game:GetService("DataStoreService")
local MemoryStoreService = game:GetService("MemoryStoreService")
local MessagingService = game:GetService("MessagingService")
local Players = game:GetService("Players")
local HttpService = game:GetService("HttpService")
local RunService = game:GetService("RunService")

local ReliableDataStore = {}
ReliableDataStore.__index = ReliableDataStore

---------------------------------------------------------------------
-- Constants
---------------------------------------------------------------------
local MAX_RETRIES = 5
local BASE_RETRY_DELAY = 0.5
local MAX_RETRY_DELAY = 10
local DATASTORE_BUDGET_REFRESH = 60
local VERSION_HISTORY_LIMIT = 5

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
		if type(t[k]) ~= "table" then
			t[k] = {}
		end
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
		if t == nil then return nil end
	end
	return t
end

-- Exponential backoff with jitter
local function retryWithBackoff(fn, maxRetries, context)
	maxRetries = maxRetries or MAX_RETRIES
	local lastErr
	
	for attempt = 1, maxRetries do
		local ok, res = pcall(fn)
		if ok then return true, res end
		
		lastErr = res
		local errStr = tostring(res)
		
		-- Don't retry on certain errors
		if errStr:match("not valid UTF%-8") or errStr:match("must be valid UTF") then
			warn("[ReliableDataStore] Non-retryable error:", errStr)
			return false, res
		end
		
		if attempt < maxRetries then
			-- Exponential backoff with jitter
			local delay = math.min(BASE_RETRY_DELAY * (2 ^ (attempt - 1)), MAX_RETRY_DELAY)
			delay = delay * (0.5 + math.random() * 0.5) -- Add jitter
			task.wait(delay)
		end
	end
	
	return false, lastErr
end

-- Budget tracking for DataStore operations
local DataStoreBudget = {}
DataStoreBudget.__index = DataStoreBudget

function DataStoreBudget.new()
	local self = setmetatable({
		reads = 0,
		writes = 0,
		lastReset = os.clock(),
	}, DataStoreBudget)
	return self
end

function DataStoreBudget:canOperate(opType)
	-- Simple throttling: reset counters every minute
	if os.clock() - self.lastReset > DATASTORE_BUDGET_REFRESH then
		self.reads = 0
		self.writes = 0
		self.lastReset = os.clock()
	end
	
	-- Conservative limits
	if opType == "read" and self.reads >= 60 then return false end
	if opType == "write" and self.writes >= 60 then return false end
	
	return true
end

function DataStoreBudget:recordOp(opType)
	if opType == "read" then
		self.reads = self.reads + 1
	elseif opType == "write" then
		self.writes = self.writes + 1
	end
end

---------------------------------------------------------------------
-- Constructor
---------------------------------------------------------------------
function ReliableDataStore.new(name, defaults, options)
	options = options or {}

	local inst = setmetatable({
		name = name,
		store = DataStoreService:GetDataStore(name),
		locks = MemoryStoreService:GetSortedMap("RDS_LOCK_" .. name),
		defaults = deepCopy(defaults or {}),
		sessions = {},
		jobId = game.JobId ~= "" and game.JobId or HttpService:GenerateGUID(false),
		budget = DataStoreBudget.new(),
		shuttingDown = false,

		-- Settings with better defaults
		settings = {
			lockTTL = options.lockTTL or 120,
			autosave = options.autosave or 60, -- Increased from 30
			retries = options.retries or MAX_RETRIES,
			backupCount = options.backupCount or 3,
			sessionTimeout = options.sessionTimeout or 600,
			enableBudgetTracking = options.enableBudgetTracking ~= false,
			gracefulShutdownTimeout = options.gracefulShutdownTimeout or 30,
		},

		validators = {},
		schema = options.schema,
		migrations = {},
		compressor = options.compressor or { 
			enabled = false, 
			encode = function(x) return x end, 
			decode = function(x) return x end 
		},

		events = {
			OnLoaded = Instance.new("BindableEvent"),
			OnSaved = Instance.new("BindableEvent"),
			OnKicked = Instance.new("BindableEvent"),
			OnConflict = Instance.new("BindableEvent"),
			OnError = Instance.new("BindableEvent"),
		},
	}, ReliableDataStore)

	return inst
end

function ReliableDataStore:On(event)
	return self.events[event].Event
end

function ReliableDataStore:Log(level, msg, plr)
	local timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ")
	local playerInfo = plr and (" [%s:%d]"):format(plr.Name, plr.UserId) or ""
	local message = ("[%s] [ReliableDataStore:%s]%s %s"):format(level, self.name, playerInfo, tostring(msg))
	
	if level == "ERROR" then
		warn(message)
		self.events.OnError:Fire(msg, plr)
	else
		print(message)
	end
end

---------------------------------------------------------------------
-- Validators / Schema
---------------------------------------------------------------------
function ReliableDataStore:SetValidator(key, fn)
	assert(type(fn) == "function", "Validator must be a function")
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
			
			if spec.min and type(val) == "number" and val < spec.min then
				return false, ("%s must be >= %s"):format(p, spec.min)
			end
			
			if spec.max and type(val) == "number" and val > spec.max then
				return false, ("%s must be <= %s"):format(p, spec.max)
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
	for v in pairs(self.migrations) do 
		table.insert(versions, v) 
	end
	table.sort(versions)
	
	for _, v in ipairs(versions) do
		if v > current then
			local ok, res = pcall(self.migrations[v], data)
			if ok then
				data = res or data
				data._schemaVersion = v
				self:Log("INFO", ("Applied migration to version %d"):format(v))
			else
				self:Log("ERROR", ("Migration %d failed: %s"):format(v, tostring(res)))
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
			if not cur then
				return { owner = job, ts = os.clock(), acquired = os.time() }
			end
			
			if cur.owner == job then
				return { owner = job, ts = os.clock(), acquired = cur.acquired }
			end
			
			-- Detect stale lock (no heartbeat for 2x TTL)
			if type(cur.ts) == "number" and (os.clock() - cur.ts) > (ttl * 2) then
				self:Log("WARN", ("Stealing stale lock for %s (owner=%s, age=%ds)"):format(
					key, tostring(cur.owner), os.clock() - cur.ts
				))
				return { owner = job, ts = os.clock(), acquired = os.time() }
			end
			
			return nil -- Lock held by another job
		end, ttl)
	end)
	
	if not ok then
		self:Log("ERROR", ("Lock operation failed for %s: %s"):format(key, tostring(res)))
		return false
	end
	
	return res ~= nil and res.owner == job
end

function ReliableDataStore:_releaseLock(key)
	pcall(function() 
		self.locks:RemoveAsync(key) 
	end)
end

---------------------------------------------------------------------
-- Decompression helper
---------------------------------------------------------------------
local function decompressData(self, raw)
	if type(raw) ~= "table" then return raw end
	
	if raw.__compressed and raw.payload then
		local ok, decoded = pcall(function()
			local json = self.compressor.decode(raw.payload)
			return HttpService:JSONDecode(json)
		end)
		
		if ok and type(decoded) == "table" then
			return decoded
		else
			self:Log("WARN", "Failed to decompress data, using raw")
			return raw
		end
	end
	
	return raw
end

---------------------------------------------------------------------
-- Load (with migrations, decompression, schema validation)
---------------------------------------------------------------------
function ReliableDataStore:_load(plr)
	local key = "u:" .. plr.UserId
	
	-- Acquire lock
	if not self:_lock(key) then
		plr:Kick("Your data is currently in use. Please try again in a moment.")
		self.events.OnKicked:Fire(plr)
		return
	end

	-- Check budget
	if self.settings.enableBudgetTracking and not self.budget:canOperate("read") then
		self:Log("WARN", "DataStore budget exhausted, delaying load", plr)
		task.wait(5)
	end

	-- Load from DataStore
	local raw
	local success, err = retryWithBackoff(function()
		raw = self.store:GetAsync(key)
	end, self.settings.retries, "Load")
	
	if self.settings.enableBudgetTracking then
		self.budget:recordOp("read")
	end

	if not success then
		self:Log("ERROR", ("Failed to load data: %s"):format(tostring(err)), plr)
		plr:Kick("Failed to load your data. Please try again.")
		self:_releaseLock(key)
		return
	end

	-- Initialize or decompress data
	local data
	if not raw then
		data = deepCopy(self.defaults)
		data._version = 1
		data._schemaVersion = 0
		data._metaCreatedAt = os.time()
	else
		data = decompressData(self, raw)
		
		if type(data) ~= "table" then 
			self:Log("WARN", "Invalid data format, using defaults", plr)
			data = deepCopy(self.defaults) 
		end
		
		deepMerge(data, deepCopy(self.defaults))
		data._version = (data._version or 0) + 1
	end

	-- Apply migrations
	local ok, migrated = pcall(function() 
		return applyMigrations(self, data) 
	end)
	if ok and type(migrated) == "table" then 
		data = migrated 
	end

	-- Validate schema
	local valid, validErr = self:ValidateSchema(data)
	if not valid then
		self:Log("WARN", ("Schema validation failed: %s"):format(tostring(validErr)), plr)
	end

	-- Create session
	self.sessions[plr] = {
		data = data,
		version = data._version,
		dirty = {},
		backups = {},
		meta = {
			CreatedAt = data._metaCreatedAt or os.time(),
			LastLoaded = os.time(),
			LastSave = 0,
			LastHeartbeat = os.clock(),
			LoadAttempts = (data._metaLoadAttempts or 0) + 1,
		},
	}

	self:Log("INFO", ("Loaded data (version=%d, attempts=%d)"):format(
		data._version, self.sessions[plr].meta.LoadAttempts
	), plr)
	
	self.events.OnLoaded:Fire(plr, deepCopy(data))
end

---------------------------------------------------------------------
-- Public API: Get / Set / Increment / Export / Import
---------------------------------------------------------------------
function ReliableDataStore:Get(plr, key)
	local session = self.sessions[plr]
	if not session then return nil end
	session.meta.LastHeartbeat = os.clock()
	return key and deepGet(session.data, key) or session.data
end

function ReliableDataStore:Set(plr, key, value)
	local session = self.sessions[plr]
	if not session then 
		self:Log("WARN", "Attempted to set data for player without session", plr)
		return false 
	end

	-- Validate
	if key and self.validators[key] then
		local ok, err = pcall(self.validators[key], value)
		if not ok or not err then
			self:Log("WARN", ("Validation failed for %s: %s"):format(key, tostring(err or "returned false")), plr)
			return false
		end
	end

	if not key then
		session.data = value
		session.dirty["_root"] = true
	else
		deepSet(session.data, key, value)
		session.dirty[key] = true
	end

	session.version = session.version + 1
	session.meta.LastHeartbeat = os.clock()
	return true
end

-- Safe numeric increment
function ReliableDataStore:Increment(plr, key, amount)
	amount = amount or 1
	local current = self:Get(plr, key)
	
	if type(current) ~= "number" then
		self:Log("WARN", ("Cannot increment non-number at %s"):format(key), plr)
		return false
	end
	
	return self:Set(plr, key, current + amount)
end

function ReliableDataStore:Export(plr)
	local session = self.sessions[plr]
	if not session then return nil end
	return HttpService:JSONEncode(session.data)
end

function ReliableDataStore:Import(plr, json)
	local ok, decoded = pcall(HttpService.JSONDecode, HttpService, json)
	if not ok then 
		self:Log("ERROR", "Failed to decode import data", plr)
		return false 
	end
	
	local session = self.sessions[plr]
	if not session then return false end
	
	-- Validate imported data
	local valid, err = self:ValidateSchema(decoded)
	if not valid then
		self:Log("ERROR", ("Imported data failed validation: %s"):format(tostring(err)), plr)
		return false
	end
	
	session.data = decoded
	session.version = session.version + 1
	session.dirty["_root"] = true
	return true
end

---------------------------------------------------------------------
-- Delta-save helper
---------------------------------------------------------------------
local function applyDeltasToOld(old, newData, dirtyPaths)
	if dirtyPaths["_root"] then
		return deepCopy(newData)
	end
	
	old = type(old) == "table" and old or {}
	local patched = deepCopy(old)
	
	for path in pairs(dirtyPaths) do
		if path ~= "_root" then
			local val = deepGet(newData, path)
			deepSet(patched, path, deepCopy(val))
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
	saveSnapshot._metaLoadAttempts = session.meta.LoadAttempts
	saveSnapshot._metaLastSaved = os.time()

	local dirty = session.dirty or {}
	local hasDirty = next(dirty) ~= nil

	-- Check budget
	if self.settings.enableBudgetTracking and not self.budget:canOperate("write") then
		self:Log("WARN", "DataStore budget exhausted, delaying save", plr)
		task.wait(5)
	end

	local success, err = retryWithBackoff(function()
		return self.store:UpdateAsync(key, function(old)
			old = type(old) == "table" and old or {}
			old = decompressData(self, old)

			-- Conflict detection
			if (old._version or 0) > saveSnapshot._version then
				self:Log("WARN", ("Conflict: store version %d > local %d"):format(
					old._version, saveSnapshot._version
				), plr)
				self.events.OnConflict:Fire(plr, old, saveSnapshot)
				return old
			end

			local toWrite
			if not hasDirty then
				toWrite = old
			else
				toWrite = applyDeltasToOld(old, saveSnapshot, dirty)
				toWrite._version = saveSnapshot._version
				toWrite._metaLastSaved = saveSnapshot._metaLastSaved
				toWrite._metaLoadAttempts = saveSnapshot._metaLoadAttempts
			end

			-- Compress if enabled
			if self.compressor and self.compressor.enabled then
				local json = HttpService:JSONEncode(toWrite)
				local okc, payload = pcall(self.compressor.encode, json)
				if okc then
					return { __compressed = true, payload = payload }
				end
			end

			return toWrite
		end)
	end, self.settings.retries, "Save")
	
	if self.settings.enableBudgetTracking then
		self.budget:recordOp("write")
	end

	if not success then
		self:Log("ERROR", ("Failed to save: %s"):format(tostring(err)), plr)
	else
		-- Rotate backups
		if hasDirty then
			table.insert(session.backups, 1, deepCopy(saveSnapshot))
			while #session.backups > self.settings.backupCount do
				table.remove(session.backups)
			end
		end
		
		session.dirty = {}
		session.meta.LastSave = os.time()
		self:Log("INFO", "Saved successfully", plr)
		self.events.OnSaved:Fire(plr, saveSnapshot)
	end

	if release then
		self:_releaseLock(key)
		self.sessions[plr] = nil
	end
end

function ReliableDataStore:ForceSave(plr)
	self:_save(plr, false)
end

function ReliableDataStore:_saveAll()
	local players = {}
	for plr in pairs(self.sessions) do
		table.insert(players, plr)
	end
	
	self:Log("INFO", ("Saving all sessions (%d players)"):format(#players))
	
	for _, plr in ipairs(players) do
		local ok, err = pcall(function()
			self:_save(plr, true)
		end)
		if not ok then
			self:Log("ERROR", ("Failed to save during shutdown: %s"):format(tostring(err)), plr)
		end
	end
end

---------------------------------------------------------------------
-- Lifecycle
---------------------------------------------------------------------
function ReliableDataStore:Start()
	if self._started then
		warn("[ReliableDataStore] Already started")
		return
	end
	self._started = true

	-- Player lifecycle
	Players.PlayerAdded:Connect(function(plr) 
		pcall(function() self:_load(plr) end)
	end)
	
	Players.PlayerRemoving:Connect(function(plr) 
		pcall(function() self:_save(plr, true) end)
	end)
	
	-- Graceful shutdown
	game:BindToClose(function() 
		self.shuttingDown = true
		self:_saveAll()
		
		-- Wait for saves to complete
		local timeout = self.settings.gracefulShutdownTimeout
		local elapsed = 0
		while next(self.sessions) and elapsed < timeout do
			task.wait(0.1)
			elapsed = elapsed + 0.1
		end
	end)

	-- Autosave loop
	task.spawn(function()
		while task.wait(self.settings.autosave) do
			if self.shuttingDown then break end
			
			for plr in pairs(self.sessions) do
				pcall(function() 
					self:_save(plr, false) 
				end)
			end
		end
	end)

	-- Lock renewal & session monitoring
	task.spawn(function()
		while task.wait(self.settings.lockTTL / 3) do
			if self.shuttingDown then break end
			
			for plr, s in pairs(self.sessions) do
				pcall(function()
					local key = "u:" .. plr.UserId
					self:_lock(key, true)
					
					-- Check for session timeout
					local inactive = os.clock() - (s.meta.LastHeartbeat or 0)
					if inactive > self.settings.sessionTimeout then
						self:Log("WARN", ("Session timeout after %ds of inactivity"):format(inactive), plr)
						self:_save(plr, true)
					end
				end)
			end
		end
	end)

	self:Log("INFO", "Started successfully")
end

---------------------------------------------------------------------
-- Utility methods
---------------------------------------------------------------------
function ReliableDataStore:GetBackup(plr, index)
	local session = self.sessions[plr]
	if not session then return nil end
	return session.backups[index or 1]
end

function ReliableDataStore:RestoreBackup(plr, index)
	local backup = self:GetBackup(plr, index)
	if not backup then return false end
	
	local session = self.sessions[plr]
	session.data = deepCopy(backup)
	session.version = session.version + 1
	session.dirty["_root"] = true
	
	self:Log("INFO", ("Restored backup %d"):format(index or 1), plr)
	return true
end

return ReliableDataStore
