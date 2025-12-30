local DataStoreService = game:GetService("DataStoreService")
local MemoryStoreService = game:GetService("MemoryStoreService")
local MessagingService = game:GetService("MessagingService")
local Players = game:GetService("Players")
local HttpService = game:GetService("HttpService")
local RunService = game:GetService("RunService")

local ReliableDataStore = {}
ReliableDataStore.__index = ReliableDataStore

---------------------------------------------------------------------
-- Constants (ProfileStore-inspired)
---------------------------------------------------------------------
local AUTO_SAVE_PERIOD = 300 -- 5 minutes (ProfileStore uses this)
local FIRST_LOAD_REPEAT = 5
local LOAD_REPEAT_PERIOD = 10
local SESSION_STEAL_TIMEOUT = 40
local ASSUME_DEAD_SESSION = 630 -- 10.5 minutes
local START_SESSION_TIMEOUT = 120

local MAX_RETRIES = 5
local BASE_RETRY_DELAY = 0.5
local MAX_RETRY_DELAY = 10

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
			local delay = math.min(BASE_RETRY_DELAY * (2 ^ (attempt - 1)), MAX_RETRY_DELAY)
			delay = delay * (0.5 + math.random() * 0.5)
			task.wait(delay)
		end
	end
	
	return false, lastErr
end

---------------------------------------------------------------------
-- Signal Implementation (for events)
---------------------------------------------------------------------
local Signal = {}
Signal.__index = Signal

function Signal.new()
	local self = setmetatable({
		_connections = {}
	}, Signal)
	return self
end

function Signal:Connect(fn)
	local connection = {
		Connected = true,
		_fn = fn,
		_signal = self
	}
	
	function connection:Disconnect()
		self.Connected = false
		local index = table.find(self._signal._connections, self)
		if index then
			table.remove(self._signal._connections, index)
		end
	end
	
	table.insert(self._connections, connection)
	return connection
end

function Signal:Fire(...)
	for _, conn in ipairs(self._connections) do
		if conn.Connected then
			task.spawn(conn._fn, ...)
		end
	end
end

---------------------------------------------------------------------
-- GlobalUpdates System (ProfileService feature)
---------------------------------------------------------------------
local GlobalUpdates = {}
GlobalUpdates.__index = GlobalUpdates

function GlobalUpdates.new(profile)
	local self = setmetatable({
		_profile = profile,
		_active_updates = {},
		_locked_updates = {},
		_update_index = 0,
		OnNewActiveUpdate = Signal.new(),
		OnNewLockedUpdate = Signal.new(),
	}, GlobalUpdates)
	return self
end

function GlobalUpdates:GetActiveUpdates()
	return deepCopy(self._active_updates)
end

function GlobalUpdates:GetLockedUpdates()
	return deepCopy(self._locked_updates)
end

function GlobalUpdates:ListenToNewActiveUpdate(listener)
	return self.OnNewActiveUpdate:Connect(listener)
end

function GlobalUpdates:ListenToNewLockedUpdate(listener)
	return self.OnNewLockedUpdate:Connect(listener)
end

function GlobalUpdates:LockActiveUpdate(update_id)
	if not self._profile:IsActive() then
		error("[RDS] Cannot lock update on inactive profile")
	end
	
	for i, update in ipairs(self._active_updates) do
		if update.id == update_id then
			table.remove(self._active_updates, i)
			table.insert(self._locked_updates, update)
			self.OnNewLockedUpdate:Fire(update.id, update.data)
			return
		end
	end
end

function GlobalUpdates:ClearLockedUpdate(update_id)
	if not self._profile:IsActive() then
		error("[RDS] Cannot clear update on inactive profile")
	end
	
	for i, update in ipairs(self._locked_updates) do
		if update.id == update_id then
			table.remove(self._locked_updates, i)
			return
		end
	end
end

function GlobalUpdates:AddActiveUpdate(update_data)
	self._update_index = self._update_index + 1
	local update = {
		id = self._update_index,
		data = update_data,
		timestamp = os.time()
	}
	table.insert(self._active_updates, update)
	return update.id
end

function GlobalUpdates:_serialize()
	return {
		update_index = self._update_index,
		active = self._active_updates,
		locked = self._locked_updates,
	}
end

function GlobalUpdates:_deserialize(data)
	if not data then return end
	self._update_index = data.update_index or 0
	self._active_updates = data.active or {}
	self._locked_updates = data.locked or {}
end

---------------------------------------------------------------------
-- Profile Object (ProfileService-style)
---------------------------------------------------------------------
local Profile = {}
Profile.__index = Profile

function Profile.new(store, key, data)
	local self = setmetatable({
		Data = data,
		Key = key,
		Store = store,
		MetaData = {
			MetaTags = {},
			LastUpdate = os.time(),
			LoadCount = 0,
		},
		GlobalUpdates = GlobalUpdates.new(),
		LastSavedData = nil,
		
		-- Internal
		_session_id = HttpService:GenerateGUID(false),
		_active = false,
		_load_count = 0,
		_last_save_time = 0,
		_msg_subscription = nil,
		
		-- Events
		OnSave = Signal.new(),
		OnLastSave = Signal.new(),
	}, Profile)
	
	-- Deserialize GlobalUpdates if they exist
	if data._GlobalUpdates then
		self.GlobalUpdates:_deserialize(data._GlobalUpdates)
		data._GlobalUpdates = nil
	end
	
	return self
end

function Profile:IsActive()
	return self._active
end

function Profile:GetMetaTag(tag_name)
	return self.MetaData.MetaTags[tag_name]
end

function Profile:SetMetaTag(tag_name, value)
	if not self:IsActive() then
		error("[RDS] Cannot set MetaTag on inactive profile")
	end
	self.MetaData.MetaTags[tag_name] = value
end

function Profile:Reconcile()
	if not self:IsActive() then
		error("[RDS] Cannot reconcile inactive profile")
	end
	deepMerge(self.Data, deepCopy(self.Store.defaults))
end

function Profile:Save()
	if not self:IsActive() then
		warn("[RDS] Attempted to save inactive profile")
		return
	end
	self.Store:_saveProfile(self, false)
end

function Profile:Release()
	if not self:IsActive() then
		return
	end
	self.Store:_saveProfile(self, true)
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
		profiles = {},
		jobId = game.JobId ~= "" and game.JobId or HttpService:GenerateGUID(false),
		shuttingDown = false,

		-- Settings (ProfileStore-inspired)
		settings = {
			autoSave = options.autoSave or AUTO_SAVE_PERIOD,
			lockTTL = options.lockTTL or 120,
			retries = options.retries or MAX_RETRIES,
			useMessagingService = options.useMessagingService ~= false,
			sessionStealTimeout = options.sessionStealTimeout or SESSION_STEAL_TIMEOUT,
			assumeDeadSession = options.assumeDeadSession or ASSUME_DEAD_SESSION,
		},

		validators = {},
		schema = options.schema,
		migrations = {},
		compressor = options.compressor or { 
			enabled = false, 
			encode = function(x) return x end, 
			decode = function(x) return x end 
		},

		-- Events
		OnProfileLoaded = Signal.new(),
		OnProfileSaved = Signal.new(),
		OnProfileReleased = Signal.new(),
		OnError = Signal.new(),
		OnCriticalState = Signal.new(),
		
		-- Issue tracking
		_issue_queue = {},
		_critical_state = false,
	}, ReliableDataStore)

	return inst
end

function ReliableDataStore:On(event)
	return self[event]
end

function ReliableDataStore:Log(level, msg, profile)
	local timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ")
	local profileInfo = profile and (" [%s]"):format(profile.Key) or ""
	local message = ("[%s] [RDS:%s]%s %s"):format(level, self.name, profileInfo, tostring(msg))
	
	if level == "ERROR" then
		warn(message)
		self.OnError:Fire(msg, profile)
		self:_trackIssue()
	else
		print(message)
	end
end

---------------------------------------------------------------------
-- Issue Tracking & Critical State (ProfileService feature)
---------------------------------------------------------------------
function ReliableDataStore:_trackIssue()
	local now = os.time()
	table.insert(self._issue_queue, now)
	
	-- Remove old issues (> 2 minutes)
	while #self._issue_queue > 0 and (now - self._issue_queue[1]) > 120 do
		table.remove(self._issue_queue, 1)
	end
	
	-- Check for critical state
	if #self._issue_queue >= 5 and not self._critical_state then
		self._critical_state = true
		self.OnCriticalState:Fire()
		self:Log("CRITICAL", "Entered critical state - experiencing multiple DataStore errors")
		
		-- Auto-recover after 2 minutes
		task.delay(120, function()
			self._critical_state = false
			self._issue_queue = {}
		end)
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
-- MessagingService Integration (ProfileStore feature)
---------------------------------------------------------------------
function ReliableDataStore:_setupMessaging(profile)
	if not self.settings.useMessagingService then return end
	
	local topic = "RDS_" .. profile.Key
	
	-- Protect against MessagingService blocking
	local success = pcall(function()
		profile._msg_subscription = MessagingService:SubscribeAsync(topic, function(message)
			if type(message.Data) ~= "table" then return end
			if message.Data.session_id == profile._session_id then return end
			
			-- Another server wants this profile
			if message.Data.action == "request_release" then
				self:Log("INFO", "Received session release request via MessagingService", profile)
				profile:Release()
			end
		end)
	end)
	
	if not success then
		self:Log("WARN", "MessagingService subscription failed, falling back to polling", profile)
	end
end

function ReliableDataStore:_requestRelease(key)
	if not self.settings.useMessagingService then return end
	
	local topic = "RDS_" .. key
	pcall(function()
		MessagingService:PublishAsync(topic, {
			action = "request_release",
			session_id = HttpService:GenerateGUID(false),
			timestamp = os.time()
		})
	end)
end

---------------------------------------------------------------------
-- Session Locking (Enhanced with MessagingService)
---------------------------------------------------------------------
function ReliableDataStore:_acquireLock(key, session_id)
	local ttl = self.settings.lockTTL
	
	local ok, res = pcall(function()
		return self.locks:UpdateAsync(key, function(cur)
			if not cur then
				return {
					owner = self.jobId,
					session_id = session_id,
					ts = os.time(),
					last_update = os.time()
				}
			end
			
			-- We already own it
			if cur.owner == self.jobId and cur.session_id == session_id then
				return {
					owner = self.jobId,
					session_id = session_id,
					ts = cur.ts,
					last_update = os.time()
				}
			end
			
			-- Check if lock is stale
			local age = os.time() - (cur.last_update or cur.ts or 0)
			if age > self.settings.assumeDeadSession then
				self:Log("WARN", ("Stealing stale lock (age=%ds)"):format(age))
				return {
					owner = self.jobId,
					session_id = session_id,
					ts = os.time(),
					last_update = os.time()
				}
			end
			
			return nil -- Lock held by another server
		end, ttl)
	end)
	
	if not ok then
		self:Log("ERROR", ("Lock operation failed: %s"):format(tostring(res)))
		return false
	end
	
	return res ~= nil and res.owner == self.jobId
end

function ReliableDataStore:_releaseLock(key)
	pcall(function() 
		self.locks:RemoveAsync(key) 
	end)
end

function ReliableDataStore:_refreshLock(key, session_id)
	return self:_acquireLock(key, session_id)
end

---------------------------------------------------------------------
-- Data Compression/Decompression
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
			self:Log("WARN", "Failed to decompress data")
			return raw
		end
	end
	
	return raw
end

local function compressData(self, data)
	if not self.compressor or not self.compressor.enabled then
		return data
	end
	
	local ok, payload = pcall(function()
		local json = HttpService:JSONEncode(data)
		return self.compressor.encode(json)
	end)
	
	if ok then
		return { __compressed = true, payload = payload }
	end
	
	return data
end

---------------------------------------------------------------------
-- Load Profile (ProfileStore-style with conflict resolution)
---------------------------------------------------------------------
function ReliableDataStore:LoadProfileAsync(key, notReleasedHandler)
	local fullKey = "u:" .. key
	local session_id = HttpService:GenerateGUID(false)
	local load_attempts = 0
	local start_time = os.clock()
	
	notReleasedHandler = notReleasedHandler or "ForceLoad"
	
	while true do
		load_attempts = load_attempts + 1
		
		-- Check for shutdown
		if self.shuttingDown then
			self:Log("INFO", "Load cancelled - server shutting down")
			return nil
		end
		
		-- Timeout check
		if os.clock() - start_time > START_SESSION_TIMEOUT then
			self:Log("ERROR", "Load timeout after " .. START_SESSION_TIMEOUT .. "s")
			return nil
		end
		
		-- Try to acquire lock
		if self:_acquireLock(fullKey, session_id) then
			-- Load data
			local raw
			local success, err = retryWithBackoff(function()
				raw = self.store:GetAsync(fullKey)
			end, self.settings.retries)
			
			if not success then
				self:Log("ERROR", "Failed to load: " .. tostring(err))
				self:_releaseLock(fullKey)
				return nil
			end
			
			-- Initialize or decompress data
			local data
			if not raw then
				data = deepCopy(self.defaults)
				data._version = 1
				data._schemaVersion = 0
			else
				data = decompressData(self, raw)
				
				if type(data) ~= "table" then 
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
				self:Log("WARN", "Schema validation failed: " .. tostring(validErr))
			end
			
			-- Create profile
			local profile = Profile.new(self, key, data)
			profile._session_id = session_id
			profile._active = true
			profile._load_count = load_attempts
			profile.LastSavedData = deepCopy(data)
			profile.MetaData.LoadCount = load_attempts
			
			self.profiles[key] = profile
			
			-- Setup MessagingService
			self:_setupMessaging(profile)
			
			self:Log("INFO", ("Loaded profile in %d attempts"):format(load_attempts))
			self.OnProfileLoaded:Fire(profile)
			
			return profile
		else
			-- Lock is held by another server
			if load_attempts == 1 then
				-- First attempt - send release request via MessagingService
				self:_requestRelease(fullKey)
				task.wait(FIRST_LOAD_REPEAT)
			elseif load_attempts * LOAD_REPEAT_PERIOD >= self.settings.sessionStealTimeout then
				-- Timeout - handle based on notReleasedHandler
				if type(notReleasedHandler) == "function" then
					local result = notReleasedHandler(nil, nil)
					if result == "Cancel" then
						return nil
					elseif result == "Steal" or result == "ForceLoad" then
						-- Force acquire by clearing lock
						self:_releaseLock(fullKey)
						task.wait(1)
						-- Loop will retry
					end
				elseif notReleasedHandler == "ForceLoad" or notReleasedHandler == "Steal" then
					self:_releaseLock(fullKey)
					task.wait(1)
				else
					return nil
				end
			else
				-- Keep waiting
				self:_requestRelease(fullKey)
				task.wait(LOAD_REPEAT_PERIOD)
			end
		end
	end
end

---------------------------------------------------------------------
-- GlobalUpdate API (send updates to offline profiles)
---------------------------------------------------------------------
function ReliableDataStore:GlobalUpdateProfileAsync(key, updateHandler)
	local fullKey = "u:" .. key
	
	local success, result = retryWithBackoff(function()
		return self.store:UpdateAsync(fullKey, function(data)
			data = data or {}
			data = decompressData(self, data)
			
			if type(data) ~= "table" then
				data = {}
			end
			
			-- Create temporary GlobalUpdates
			local globalUpdates = GlobalUpdates.new()
			globalUpdates:_deserialize(data._GlobalUpdates)
			
			-- Let handler add updates
			updateHandler(globalUpdates)
			
			-- Serialize back
			data._GlobalUpdates = globalUpdates:_serialize()
			
			return compressData(self, data)
		end)
	end, self.settings.retries)
	
	if not success then
		self:Log("ERROR", "GlobalUpdate failed: " .. tostring(result))
		return nil
	end
	
	return true
end

---------------------------------------------------------------------
-- Save Profile
---------------------------------------------------------------------
function ReliableDataStore:_saveProfile(profile, release)
	if not profile:IsActive() then
		return
	end
	
	local fullKey = "u:" .. profile.Key
	
	-- Fire OnSave signal (allows modifications before save)
	profile.OnSave:Fire()
	
	-- Serialize GlobalUpdates into data
	profile.Data._GlobalUpdates = profile.GlobalUpdates:_serialize()
	profile.Data._version = (profile.Data._version or 0) + 1
	profile.MetaData.LastUpdate = os.time()
	
	local saveData = deepCopy(profile.Data)
	saveData._MetaData = profile.MetaData
	
	local success, err = retryWithBackoff(function()
		return self.store:UpdateAsync(fullKey, function(old)
			return compressData(self, saveData)
		end)
	end, self.settings.retries)
	
	if not success then
		self:Log("ERROR", "Save failed: " .. tostring(err), profile)
		return
	end
	
	profile.LastSavedData = deepCopy(profile.Data)
	profile._last_save_time = os.time()
	
	self:Log("INFO", "Saved successfully", profile)
	self.OnProfileSaved:Fire(profile)
	
	if release then
		-- Fire OnLastSave signal
		profile.OnLastSave:Fire()
		
		-- Cleanup
		profile._active = false
		if profile._msg_subscription then
			pcall(function() profile._msg_subscription:Disconnect() end)
		end
		self:_releaseLock(fullKey)
		self.profiles[profile.Key] = nil
		
		self.OnProfileReleased:Fire(profile)
		self:Log("INFO", "Released profile", profile)
	end
end

---------------------------------------------------------------------
-- Public API: Backwards Compatible Wrappers
---------------------------------------------------------------------
function ReliableDataStore:Get(plr, key)
	local profile = self.profiles[tostring(plr.UserId)]
	if not profile then return nil end
	return key and deepGet(profile.Data, key) or profile.Data
end

function ReliableDataStore:Set(plr, key, value)
	local profile = self.profiles[tostring(plr.UserId)]
	if not profile then return false end
	
	-- Validate
	if key and self.validators[key] then
		local ok, err = pcall(self.validators[key], value)
		if not ok or not err then
			self:Log("WARN", ("Validation failed for %s"):format(key), profile)
			return false
		end
	end
	
	if not key then
		profile.Data = value
	else
		deepSet(profile.Data, key, value)
	end
	
	return true
end

function ReliableDataStore:Increment(plr, key, amount)
	local current = self:Get(plr, key)
	if type(current) ~= "number" then return false end
	return self:Set(plr, key, current + (amount or 1))
end

function ReliableDataStore:ForceSave(plr)
	local profile = self.profiles[tostring(plr.UserId)]
	if profile then
		profile:Save()
	end
end

---------------------------------------------------------------------
-- Lifecycle
---------------------------------------------------------------------
function ReliableDataStore:Start()
	if self._started then
		warn("[RDS] Already started")
		return
	end
	self._started = true

	-- Player lifecycle
	Players.PlayerAdded:Connect(function(plr)
		local profile = self:LoadProfileAsync(tostring(plr.UserId))
		if not profile then
			plr:Kick("Failed to load your data. Please rejoin.")
		end
	end)
	
	Players.PlayerRemoving:Connect(function(plr)
		local profile = self.profiles[tostring(plr.UserId)]
		if profile then
			profile:Release()
		end
	end)
	
	-- Graceful shutdown
	game:BindToClose(function()
		self.shuttingDown = true
		self:Log("INFO", "Server shutting down, saving all profiles...")
		
		local profiles = {}
		for _, profile in pairs(self.profiles) do
			table.insert(profiles, profile)
		end
		
		for _, profile in ipairs(profiles) do
			pcall(function()
				profile:Release()
			end)
		end
		
		-- Wait for saves
		local timeout = 30
		local elapsed = 0
		while next(self.profiles) and elapsed < timeout do
			task.wait(0.1)
			elapsed = elapsed + 0.1
		end
	end)

	-- Auto-save loop (ProfileStore uses 300s)
	task.spawn(function()
		local profileList = {}
		while task.wait(self.settings.autoSave / 100) do -- Spread saves
			if self.shuttingDown then break end
			
			-- Build profile list
			profileList = {}
			for _, profile in pairs(self.profiles) do
				table.insert(profileList, profile)
			end
			
			-- Save one profile per iteration to spread load
			if #profileList > 0 then
				local profile = profileList[(os.clock() % #profileList) + 1]
				pcall(function()
					self:_saveProfile(profile, false)
				end)
			end
		end
	end)

	-- Lock refresh loop
	task.spawn(function()
		while task.wait(self.settings.lockTTL / 3) do
			if self.shuttingDown then break end
			
			for _, profile in pairs(self.profiles) do
				pcall(function()
					self:_refreshLock("u:" .. profile.Key, profile._session_id)
				end)
			end
		end
	end)

	self:Log("INFO", "Started successfully")
end

return ReliableDataStore
