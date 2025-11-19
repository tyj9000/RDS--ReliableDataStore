# RDS – Roblox DataStore Module

**RDS** is a modern, production-ready Roblox DataStore module designed to simplify saving and loading player data with advanced features.

---

## Features

* **Delta-save system**: only writes fields that changed for efficiency.
* **Schema validation**: optional per-key or nested table validation to enforce data integrity.
* **Session timeout recovery**: detects inactive sessions and automatically saves/releases data.
* **Migration versioning**: define migrations to safely evolve your data schema.
* **Compression support**: optional hooks for JSONPack, LZ4, or custom compression.
* **Event system**: `OnLoaded`, `OnSaved`, `OnKicked`, `OnConflict`.
* **Backups**: configurable number of previous saves for each player.
---

## Installation

Place the `RDS` module in `ServerScriptService` or `ReplicatedStorage` and require it in your server scripts:

```lua
local RDS = require(game.ServerScriptService.RDS)
```

---

## Usage

### Creating a new datastore

The `options` parameter is **optional**. If not provided, default settings will be used.

```lua
local defaults = {
    Coins = 0,
    Inventory = {},
}

local options = {
    autosave = 30,        -- seconds
    lockTTL = 120,         -- lock time-to-live
    backupCount = 3,       -- how many backups to keep
    schema = {
        Coins = { type = "number", required = true },
        Inventory = { type = "table" }
    },
    compressor = {
        enabled = false,
        encode = function(str) return str end,
        decode = function(str) return str end,
    }
}

local PlayerDataStore = RDS.new("PlayerData", defaults, options) -- options optional
```

### Starting the datastore

```lua
PlayerDataStore:Start() -- automatically binds PlayerAdded, PlayerRemoving, autosave, lock renewal
```

### Accessing player data

```lua
-- Get a single key
local coins = PlayerDataStore:Get(player, "Coins")

-- Set a key safely
PlayerDataStore:Set(player, "Coins", coins + 100)

-- Export as JSON
local jsonData = PlayerDataStore:Export(player)

-- Import from JSON
PlayerDataStore:Import(player, jsonData)
```

### Increment helper (optional)

You can implement your own helper to increment safely:

```lua
local function Increment(player, key, amount)
    local current = PlayerDataStore:Get(player, key) or 0
    PlayerDataStore:Set(player, key, current + amount)
end
```

### Event listeners

```lua
PlayerDataStore:On("OnLoaded"):Connect(function(player, data)
    print(player.Name .. " data loaded")
end)

PlayerDataStore:On("OnSaved"):Connect(function(player, data)
    print(player.Name .. " data saved")
end)

PlayerDataStore:On("OnConflict"):Connect(function(player, oldData, newData)
    warn("Conflict detected for " .. player.Name)
end)
```

### Migrations

```lua
PlayerDataStore:AddMigration(1, function(data)
    data.Inventory = data.Inventory or {}
    return data
end)

PlayerDataStore:AddMigration(2, function(data)
    data._schemaVersion = 2
    return data
end)
```

### Force save manually

```lua
PlayerDataStore:_save(player) -- optionally true to release the session
```

---

## Compression Example (JSONPack)

```lua
local JSONPack = require(game.ServerScriptService.JSONPack)

local options = {
    compressor = {
        enabled = true,
        encode = function(str) return JSONPack.Pack(str) end,
        decode = function(str) return JSONPack.Unpack(str) end,
    }
}
```

---

## Notes

* Automatic locks prevent multiple sessions from overwriting each other's data.
* Stale session detection ensures that inactive players don’t lock data indefinitely.
* Delta-save and dirty-field tracking optimize performance and reduce DataStore writes.
* All original APIs (`Get`, `Set`, `Export`, `Import`) remain fully functional.

---

## License

MIT License. tyj9000 2025
