#!lua name=ermeslib

--[[
The states of a single session are the following:
    - ONLOADING     : The session is being onloaded from another node.
    - ACTIVE        : The session is active.
    - OFFLOADING    : The session is being offloaded to another node.
    - OFFLOADED     : The session has been offloaded to another node, it will
                      remain so for a while to allow the node to notify the
                      offload to the client on the following request.
    - DELETING      : The session is being deleted.
For internal functioning, in each state we track
    - non_offloadable_uses  : The number of usages that do not allow the session to be offloaded.
    - OffloadableUses       : The number of usages that allow the session to be offloaded.


The full state machine is described here:
    ONLOADING: The session is being onloaded from another node.
        - State:
            - non_offloadable_uses  : 0,
            - OffloadableUses       : 0,
        - Transitions:
            (_, _) onload_data   -> ONLOADING (_, _)}.
            (_, _) onload_finish -> ACTIVE    (_, _)}.
            (_, _) delete        -> DELETING  (_, _)}.

    ACTIVE: The session is active
        - State
            - non_offloadable_uses  : 0-N,
            - OffloadableUses       : 0-N,
        - Transitions:
            (0-N, _  ) acquire              -> ACTIVE     ($++, _  )}.
            (1-N, _  ) release              -> ACTIVE     ($--, _  )}.
            (_  , 0-N) acquire-offloadable  -> ACTIVE     (_  , $++)}.
            (_  , 1-N) release-offloadable  -> ACTIVE     (_  , $--)}.
            (0  , 0-N) offload              -> OFFLOADING (_  , _  )}.
            (0  , 0  ) delete               -> DELETING   (_  , _  )}.

    OFFLOADING: The session is being offloaded to another node.
        - State
            - non_offloadable_uses  : 0,
            - OffloadableUses       : 0-N
        - Transitions
            (_  , 0-N) acquire-offloadable  -> ACTIVE    (_  , $++)}.
            (_  , 1-N) release-offloadable  -> ACTIVE    (_  , $--)}.
            (_  , _  ) offload-finish       -> OFFLOADED (_  , _  )}.
            (_  , _  ) offload-cancel       -> ACTIVE    (_  , _  )}. + restore expiration

    OFFLOADED: The session has been offloaded to another node.
        - State
            - non_offloadable_uses  : 0,
            - OffloadableUses       : 0-N
        - Transitions
            (_  , 1-N) release-offloadable -> ACTIVE    (_  , $--)}.
--]]


-- Generate a key in the infrastructure keyspace.
local function infrastructure_key(key)
    if type(key) ~= 'string' then
        error('[Ermes]: Key must be a string, got ' .. type(key))
    end

    return 'i:node:' .. key
end
-- Generate a key in the infrastructure keyspace for the relations.
local function infrastructure_children_key(key)
    return 'i:children:' .. key
end
-- Generate a key in the infrastructure keyspace for the relations.
local function infrastructure_parent_key(key)
    return 'i:parent:' .. key
end
-- Generate a key in the config keyspace.
local function config_key(key)
    return 'c:' .. key
end
-- Generate a key in the session keyspace.
local function session_data_key(session_id, key)
    return 's:' .. session_id .. ':' .. key
end
-- Generate a key in the session metadata keyspace.
local function session_metadata_key(session_id)
    --[[
    The session metadata is stored in a hash with the following fields (Note that some properties are like score and
    expiration are stored also in the sorted sets and are kept in sync):
        'state',
        'non_offloadable_uses',
        'offloadable_uses',
        'client_lat',
        'client_long',
        'offloaded_to_host',
        'offloaded_to_session',
        'created_in',
        'created_at',
        'created_at',
        'expires_at',
        'updated_at',
    --]]
    return 'm:' .. session_id .. ':metadata'
end
-- Extract the session id from the session data key.
local function extract_key_from_session_data_key(session_data_key)
    local _, _, key = string.find(session_data_key, "s:.-:(.+)")
    return key
end

-- Ordered set by expiration (or +inf if no expiration is set) of the sessions.
-- Sessions that are being used have as score the negative of the expiration time.
-- (or -inf if no expiration is set)
local sessions_set = config_key('sessions_set')
-- Ordered set by score of the sessions that can be offloaded.
local offloadable_sessions_set = config_key('offloadable_sessions_set')
-- Ordered set by score of the sessions that are offloaded.
local offloaded_sessions_set = config_key('offloaded_sessions_set')
-- Geo set of the nodes.
local nodes_geoset = config_key('nodes_geoset')
-- Key mapped to the id of the current node.
local current_node_key = "nil"

-- TODO: create a list of errors with codes and messages.
-- Errors
local sessionNotFoundError = '1'
-- ...

-- Function that register the current node key.
redis.register_function('set_current_node_key', function(keys, args)
    -- Keys.
    local node_id = keys[1]
    -- Set the current node key.
    current_node_key = node_id
    -- Return OK.
    return 'OK'
end)

-- Function that return the current node key.
redis.register_function('get_current_node_key', function(keys, args)
    return current_node_key
end)

-- Assert that the id is not empty, otherwise raise an error.
local function assert_valid_id(id)
    if id == '' then
        error('[Ermes]: Id cannot be empty')
    end

    -- session_id must not contain ":"
    if string.find(id, ':') then
        error('[Ermes]: Id cannot contain ":"')
    end
end

-- Assert that the geo coordinates are valid, otherwise raise an error.
local function assert_valid_geo_coordinates(lat, long)
    local lat, long = tonumber(lat), tonumber(long)
    -- Check if lat and long are valid.
    if lat == nil or long == nil or lat < -90 or lat > 90 or long < -180 or long > 180 then
        error('[Ermes]: Geo coordinates are not valid' .. lat .. " " .. long)
    end
end

-- Assert that the string is nil or a valid Unix timestamp, otherwise raise an
-- error.
local function assert_valid_timestamp_string_greater_than(string_timestamp, any)
    if tonumber(string_timestamp) == nil or tonumber(string_timestamp) < (tonumber(any) or 0) then
        error('[Ermes]: Unix timestamp is not valid, must be greater than ' ..
            (tonumber(any) or 0) .. ' got ' .. string_timestamp)
    end
end

-- Function that create a session and acquire it. If a session with the same id
-- already exists, return false, otherwise return true.
redis.register_function('create_session', function(keys, args)
    -- Keys.
    local session_id = keys[1]
    -- Args.
    local client_lat = args[1]
    local client_long = args[2]
    local expires_at = args[3]
    local acquire = args[4] -- "offloadable", "non-offloadable", ""
    -- Metadata.
    local metadata_key = session_metadata_key(session_id)
    -- Get the current time.
    local time = redis.call('TIME')[1]

    -- Check if the session id is valid.
    assert_valid_id(session_id)
    if client_lat ~= "" or client_long ~= "" then
        -- Check if the client coordinates are valid.
        assert_valid_geo_coordinates(client_lat, client_long)
    end
    if expires_at ~= "" then
        -- Check if the expires_at is valid.
        assert_valid_timestamp_string_greater_than(expires_at, time)
    end

    -- If session already exists, return false.
    if redis.call('EXISTS', metadata_key) == 1 then
        return false
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', metadata_key,
        'state', 'ACTIVE',
        'non_offloadable_uses', acquire == 'non-offloadable' and '1' or '0',
        'offloadable_uses', acquire == 'offloadable' and '1' or '0',
        'client_lat', client_lat,
        'client_long', client_long,
        'created_in', current_node_key,
        'created_at', tostring(time),
        'updated_at', tostring(time),
        'expires_at', expires_at)

    if acquire ~= 'non-offloadable' then
        -- Add it to the offloadable_sessions_set.
        redis.call('ZADD', offloadable_sessions_set, time, session_id)
    end

    if acquire ~= 'non-offloadable' and acquire ~= 'offloadable' then
        -- Add it to the sessions_set.
        redis.call('ZADD', sessions_set, expires_at ~= "" and expires_at or '+inf', session_id)
    else
        -- Add it to the sessions_set.
        redis.call('ZADD', sessions_set, '-' .. (expires_at ~= "" and expires_at or 'inf'), session_id)
    end

    -- Return true.
    return true
end)

-- Function that create a session and set it for onload.
redis.register_function('onload_start', function(keys, args)
    -- Keys.
    local session_id = keys[1]
    -- Args.
    local client_lat = args[1]
    local client_long = args[2]
    local created_in = args[3]
    local created_at = args[4]
    local updated_at = args[5]
    local expires_at = args[6]
    -- Metadata.
    local metadata_key = session_metadata_key(session_id)

    -- Check if the session id is valid.
    assert_valid_id(session_id)
    -- Check if the client coordinates are valid.
    if client_lat ~= "" or client_long ~= "" then
        -- Check if the client coordinates are valid.
        assert_valid_geo_coordinates(client_lat, client_long)
    end
    -- Check if the updated_at is valid.
    assert_valid_timestamp_string_greater_than(created_at, 0)
    -- Check if the updated_at is valid.
    assert_valid_timestamp_string_greater_than(updated_at, created_at)
    if expires_at ~= "" then
        -- Check if the expires_at is valid.
        assert_valid_timestamp_string_greater_than(expires_at, created_at)
        assert_valid_timestamp_string_greater_than(expires_at, redis.call('TIME')[1])
    end

    -- If session already exists, return false.
    if redis.call('EXISTS', metadata_key) == 1 then
        return false
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', metadata_key,
        'state', 'ONLOADING',
        'non_offloadable_uses', 0,
        'offloadable_uses', 0,
        'client_lat', client_lat,
        'client_long', client_long,
        'created_in', created_in,
        'created_at', created_at,
        'updated_at', updated_at,
        'expires_at', expires_at)

    -- Add it to the sessions_set.
    redis.call('ZADD', sessions_set, '-' .. (expires_at ~= "" and expires_at or 'inf'), session_id)

    -- Return true.
    return true
end)

-- Function that set the session data after onload. This function is separeted
-- from onload_start to allow the client to send the data in batches.
-- TODO: Test if this work with "composite" data types, such as geo sets that
-- are based on zsets.
redis.register_function('onload_data', function(keys, args)
    -- Keys.
    local session_id = keys[1]
    -- Args.
    local data = cjson.decode(args[1])
    -- Metadata.
    local metadata_key = session_metadata_key(session_id)
    -- Get the current state.
    local state = redis.call('HGET', metadata_key, 'state')

    -- If session is not ONLOADING, return an error.
    if state ~= 'ONLOADING' then
        return redis.error_reply('[Ermes]: Session is not ONLOADING')
    end

    -- List of key-value pairs of type string.
    local strings = data['string']
    -- Set the session data.
    for key, value in pairs(strings) do
        redis.call('SET', session_data_key(session_id, key), value)
    end

    -- List of key-value pairs of type list.
    local lists = data['list']
    -- Set the session data.
    for key, value in pairs(lists) do
        redis.call('RPUSH', session_data_key(session_id, key), table.unpack(value))
    end

    -- List of key-value pairs of type set.
    local sets = data['set']
    -- Set the session data.
    for key, value in pairs(sets) do
        redis.call('SADD', session_data_key(session_id, key), table.unpack(value))
    end

    -- List of key-value pairs of type sorted set.
    local sortedSets = data['zset']
    -- Set the session data.
    for key, value in pairs(sortedSets) do
        redis.call('ZADD', session_data_key(session_id, key), table.unpack(value))
    end

    -- List of key-value pairs of type hash.
    local hashes = data['hash']
    -- Set the session data.
    for key, value in pairs(hashes) do
        redis.call('HMSET', session_data_key(session_id, key), table.unpack(value))
    end

    -- Return OK.
    return 'OK'
end)

-- Function that set the session as active after onload.
redis.register_function('onload_finish', function(keys, args)
    -- Keys.
    local session_id = keys[1]
    -- Metadata.
    local metadata_key = session_metadata_key(session_id)
    -- Get the current state.
    local result = redis.call('HMGET', metadata_key, 'state', 'expires_at', 'updated_at')
    local state, expires_at, updated_at = result[1], result[2], result[3]

    -- If session is not ONLOADING, return an error.
    if state ~= 'ONLOADING' then
        return redis.error_reply('[Ermes]: Session is not ONLOADING')
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', metadata_key, 'state', 'ACTIVE')
    -- Add it to the offloadable_sessions_set.
    redis.call('ZADD', offloadable_sessions_set, updated_at, session_id)
    -- Add it to the sessions_set.
    redis.call('ZADD', sessions_set, expires_at ~= "" and expires_at or '+inf', session_id)

    redis.call('ZREM', offloaded_sessions_set, session_id)

    -- Return OK.
    return 'OK'
end)

-- Function that acquire a session.
redis.register_function('acquire_session', function(keys, args)
    -- Keys.
    local session_id = keys[1]
    -- Args.
    local allow_offloading = args[1]
    local allow_while_offloading = args[1]
    -- Metadata.
    local metadata_key = session_metadata_key(session_id)

    -- Get the current state.
    local result = redis.call('HMGET', metadata_key, 'state', 'non_offloadable_uses', 'offloadable_uses',
        'offloaded_to_host', 'offloaded_to_session', 'expires_at')
    local state, non_offloadable_uses, offloadable_uses, offloaded_to_host, offloaded_to_session, expires_at =
        result[1], result[2], result[3], result[4], result[5], result[6]

    -- Parse the values.
    non_offloadable_uses, offloadable_uses = tonumber(non_offloadable_uses), tonumber(offloadable_uses)
    -- Get the current time.
    local time = redis.call('TIME')[1]

    -- If session is OFFLOADED, return the state of the session and the offloadedTo data.
    if state == 'OFFLOADED' then
        return { state, offloaded_to_host, offloaded_to_session }
    end

    -- If session is not ACTIVE or is expired, return an error.
    if allow_while_offloading ~= '1' and state == 'OFFLOADING' then
        return redis.error_reply('[Ermes]: Session is offloading')
    end

    -- If session is not ACTIVE or is expired, return an error.
    if (state ~= 'ACTIVE' and state ~= 'OFFLOADING') or (tonumber(expires_at) ~= nil and tonumber(expires_at) < time) then
        return redis.error_reply('[Ermes]: Session is not ACTIVE, is expired or does not exist')
    end

    -- Update use based on offloadable.
    if allow_offloading ~= '1' then
        non_offloadable_uses = non_offloadable_uses + 1
    else
        offloadable_uses = offloadable_uses + 1
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', metadata_key,
        'non_offloadable_uses', tostring(non_offloadable_uses),
        'offloadable_uses', tostring(offloadable_uses),
        'updated_at', tostring(time))

    if non_offloadable_uses == 1 then
        -- Remove it from the offloadable_sessions_set.
        redis.call('ZREM', offloadable_sessions_set, session_id)
    end

    -- Change the score if first usage.
    if offloadable_uses + non_offloadable_uses == 1 then
        -- Add it to the sessions_set.
        redis.call('ZADD', sessions_set, '-' .. (expires_at ~= "" and expires_at or 'inf'), session_id)
    end

    -- Return OK.
    return { state }
end)

-- Function that release a previously acquired session.
redis.register_function('release_session', function(keys, args)
    -- Keys.
    local session_id = keys[1]
    -- Args.
    local allow_offloading = args[1]
    -- Metadata.
    local metadata_key = session_metadata_key(session_id)
    -- Get the current state.
    local result = redis.call('HMGET', metadata_key, 'state', 'offloaded_to_host', 'offloaded_to_session',
        'non_offloadable_uses', 'offloadable_uses', 'updated_at', 'expires_at')
    local state, offloaded_to_host, offloaded_to_session, non_offloadable_uses, offloadable_uses, updated_at, expires_at =
        result[1], result[2], result[3], result[4], result[5], result[6], result[7]
    -- Get the current time.
    local time = redis.call('TIME')[1]

    -- Update use based on offloadable.
    if allow_offloading ~= '1' then
        -- If there are no non_offloadable_uses, return an error.
        if non_offloadable_uses == 0 then
            return redis.error_reply('[Ermes]: No non_offloadable_uses to release')
        end

        -- Update use based on offloadable.
        non_offloadable_uses = non_offloadable_uses - 1
    else
        -- If there are no offloadable_uses, return an error.
        if offloadable_uses == 0 then
            return redis.error_reply('[Ermes]: No offloadable_uses to release')
        end

        -- Update use based on offloadable.
        offloadable_uses = offloadable_uses - 1
    end


    -- Set the session metadata attributes.
    redis.call('HMSET', metadata_key,
        'non_offloadable_uses', tostring(non_offloadable_uses),
        'offloadable_uses', tostring(offloadable_uses),
        'updated_at', tostring(time))

    if non_offloadable_uses == 1 then
        -- Add it to the offloadable_sessions_set.
        redis.call('ZADD', offloadable_sessions_set, updated_at, session_id)
    end

    -- Change the score if last usage.
    if offloadable_uses + non_offloadable_uses == 0 then
        -- Add it to the sessions_set.
        redis.call('ZADD', sessions_set, expires_at ~= "" and expires_at or '+inf', session_id)
    end


    if state == 'OFFLOADED' then
        return { state, offloaded_to_host, offloaded_to_session }
    else
        return { state }
    end
end)

-- Function that start the offload of a session.
redis.register_function('offload_start', function(keys, args)
    -- Keys.
    local session_id = keys[1]
    -- Metadata.
    local metadata_key = session_metadata_key(session_id)
    -- Get the current state.
    local result = redis.call('HMGET', metadata_key, 'state', 'non_offloadable_uses', 'expires_at')
    local state, non_offloadable_uses, expires_at = result[1], result[2], result[3]
    -- Get the current time.
    local time = redis.call('TIME')[1]

    -- If session is not ACTIVE or has non_offloadable_uses or is expired, return an error.
    if state ~= 'ACTIVE' or non_offloadable_uses ~= "0" or (expires_at ~= "" and expires_at < time) then
        return redis.error_reply('[Ermes]: Session is not ACTIVE, has non_offloadable_uses or is expired')
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', metadata_key,
        'state', 'OFFLOADING')

    -- Remove it from the offloadable_sessions_set.
    redis.call('ZREM', offloadable_sessions_set, session_id)
    -- Add it to the sessions_set.
    redis.call('ZADD', sessions_set, '-' .. (expires_at ~= "" and expires_at or 'inf'), session_id)

    -- Return OK.
    return 'OK'
end)

-- Function that offload the data of a session.
-- TODO: Test if this work with "composite" data types, such as geo sets that
-- are based on zsets.
-- TODO: This should handle offload in chunks.
redis.register_function('offload_data', function(keys, args)
    -- Keys.
    local session_id = keys[1]
    -- Args.
    local cursor = args[1] == "" and "0:0" or args[1]
    -- Metadata.
    local metadata_key = session_metadata_key(session_id)
    -- Get the current state.
    local state = redis.call('HGET', metadata_key, 'state')

    -- Decompose the cursor.
    local scan_cursor, type_cursor = string.match(cursor, "^(%d):(.*)$")
    scan_cursor, type_cursor = tonumber(scan_cursor), tonumber(type_cursor)
    if not scan_cursor or not type_cursor or type_cursor < 0 or type_cursor > 4 then
        return redis.error_reply("Invalid cursor format")
    end

    -- If session is not OFFLOADING, return an error.
    if state ~= 'OFFLOADING' then
        return redis.error_reply('[Ermes]: Session is not OFFLOADING')
    end

    -- TODO: We build the strcut and then we encode it, we should build it
    -- directly in the encoded format.
    local data = {
        string = {},
        list = {},
        set = {},
        zset = {},
        hash = {}
    }

    -- TODO: find a good number for count.
    local count = 20
    -- Pattern to match the session data keys.
    local match_string_session_data_keys_pattern = session_data_key(session_id, '*')
    -- Loaders by type.
    local loaders = {
        -- Strings.
        function(cursor, match, count)
            -- Get the session data of type string.
            local result = redis.call('SCAN', cursor, 'MATCH', match, 'COUNT', count, 'TYPE', 'string')
            local new_cursor, keys = result[1], result[2]
            -- Set the session data.
            for _, key in ipairs(keys) do
                data['string'][extract_key_from_session_data_key(key)] = redis.call('GET', key)
            end
            -- Return the new cursor.
            return #keys, new_cursor
        end,
        -- Lists.
        function(cursor, match, count)
            -- Get the session data of type list.
            local result = redis.call('SCAN', cursor, 'MATCH', match, 'COUNT', count, 'TYPE', 'list')
            local new_cursor, keys = result[1], result[2]
            -- Set the session data.
            for _, key in ipairs(keys) do
                data['list'][extract_key_from_session_data_key(key)] = redis.call('LRANGE', key, 0, -1)
            end
            -- Return the new cursor.
            return #keys, new_cursor
        end,
        -- Sets.
        function(cursor, match, count)
            -- Get the session data of type set.
            local result = redis.call('SCAN', cursor, 'MATCH', match, 'COUNT', count, 'TYPE', 'set')
            local new_cursor, keys = result[1], result[2]
            -- Set the session data.
            for _, key in ipairs(keys) do
                data['set'][extract_key_from_session_data_key(key)] = redis.call('SMEMBERS', key)
            end
            -- Return the new cursor.
            return #keys, new_cursor
        end,
        -- Sorted sets.
        function(cursor, match, count)
            -- Get the session data of type zset.
            local result = redis.call('SCAN', cursor, 'MATCH', match, 'COUNT', count, 'TYPE', 'zset')
            local new_cursor, keys = result[1], result[2]
            -- Set the session data.
            for _, key in ipairs(keys) do
                data['zset'][extract_key_from_session_data_key(key)] = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
            end
            -- Return the new cursor.
            return #keys, new_cursor
        end,
        -- Hashes.
        function(cursor, match, count)
            -- Get the session data of type hash.
            local result = redis.call('SCAN', cursor, 'MATCH', match, 'COUNT', count, 'TYPE', 'hash')
            local new_cursor, keys = result[1], result[2]
            -- Set the session data.
            for _, key in ipairs(keys) do
                data['hash'][extract_key_from_session_data_key(key)] = redis.call('HGETALL', key)
            end
            -- Return the new cursor.
            return #keys, new_cursor
        end
    }

    type_cursor = type_cursor + 1
    local scanned
    while count > 0 do
        -- Get the session data of type string.
        scanned, scan_cursor = loaders[type_cursor](scan_cursor, match_string_session_data_keys_pattern, count)
        -- Decrease count by the number of keys fetched.
        count = count - scanned
        -- If cursor is 0, move to the next type.
        if scan_cursor == '0' then
            if type_cursor == 4 then
                -- Return the cursor and the data.
                break
            end

            type_cursor = type_cursor + 1
        end
    end

    -- Return the cursor and the data.
    return { cursor, cjson.encode(data) }
end)

-- Function that finish the offload of a session.
redis.register_function('offload_finish', function(keys, args)
    -- Keys.
    local session_id = keys[1]
    -- Args.
    local offloaded_to_host = args[1]
    local offloaded_to_session = args[2]
    -- Metadata.
    local metadata_key = session_metadata_key(session_id)
    -- Get the current state.
    local result = redis.call('HMGET', metadata_key, 'state', 'offloadable_uses', 'expires_at')
    local state, offloadable_uses, expires_at = result[1], result[2], result[3]

    -- If session is not OFFLOADING, return an error.
    if state ~= 'OFFLOADING' then
        return redis.error_reply('[Ermes]: Session is not OFFLOADING')
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', metadata_key,
        'state', 'OFFLOADED',
        'offloaded_to_host', offloaded_to_host,
        'offloaded_to_session', offloaded_to_session)

    if offloadable_uses == 0 then
        -- Add it to the sessions_set.
        redis.call('ZADD', sessions_set, expires_at ~= "" and expires_at or '+inf', session_id)
    end

    redis.call('ZADD', offloaded_sessions_set, expires_at ~= "" and expires_at or '+inf', session_id)

    -- Return OK.
    return 'OK'
end)

-- Function that cancel the offload of a session.
redis.register_function('offload_cancel', function(keys, args)
    -- Keys.
    local session_id = keys[1]
    -- Metadata.
    local metadata_key = session_metadata_key(session_id)
    -- Get the current state.
    local result = redis.call('HMGET', metadata_key, 'state', 'offloadable_uses', 'updated_at', 'expires_at')
    local state, offloadable_uses, updated_at, expires_at = result[1], result[2], result[3], result[4]

    -- If session is not OFFLOADING, return an error.
    if state ~= 'OFFLOADING' then
        return redis.error_reply('[Ermes]: Session is not OFFLOADING')
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', metadata_key, 'state', 'ACTIVE')

    -- Add it to the offloadable_sessions_set.
    redis.call('ZADD', offloadable_sessions_set, updated_at, session_id)

    if offloadable_uses == 0 then
        -- Add it to the sessions_set.
        redis.call('ZADD', sessions_set, expires_at ~= "" and expires_at or '+inf', session_id)
    end

    -- Return OK.
    return 'OK'
end)

-- Function that set the expiration time of a session.
redis.register_function('set_expire_time', function(keys, args)
    -- Keys.
    local session_id = keys[1]
    -- Args.
    local expires_at = args[1]
    -- Set the expire time.
    local metadata_key = session_metadata_key(session_id)

    if expires_at ~= "" then
        -- Check if the expires_at is valid.
        assert_valid_timestamp_string_greater_than(expires_at, redis.call('TIME')[1])
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', metadata_key,
        'expires_at', expires_at,
        'updated_at', redis.call('TIME')[1])

    -- Check if the session is being used or not.
    local result = redis.call('HMGET', metadata_key, 'offloadable_uses', 'non_offloadable_uses')
    local offloadable_uses, non_offloadable_uses = result[1], result[2]

    if non_offloadable_uses == 0 and offloadable_uses == 0 then
        -- Add it to the sessions_set.
        redis.call('ZADD', sessions_set, expires_at ~= "" and expires_at or '+inf', session_id)
    else
        -- Add it to the sessions_set.
        redis.call('ZADD', sessions_set, '-' .. (expires_at ~= "" and expires_at or 'inf'), session_id)
    end

    -- Return OK.
    return 'OK'
end)

-- Function that set the coordinates of the client of a session.
redis.register_function('set_client_coordinates', function(keys, args)
    -- Keys.
    local session = session_metadata_key(keys[1])
    -- Args.
    local client_lat = args[1]
    local client_long = args[2]

    if client_lat ~= "" or client_long ~= "" then
        -- Check if the client coordinates are valid.
        assert_valid_geo_coordinates(client_lat, client_long)
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', session,
        'client_lat', client_lat,
        'client_long', client_long,
        'updated_at', redis.call('TIME')[1])

    -- Return OK.
    return 'OK'
end)

local function delete_session_chunk(session_id, count)
    -- Metadata.
    local metadata_key = session_metadata_key(session_id)
    -- Get the current state.
    local result = redis.call('HMGET', metadata_key, 'state', 'non_offloadable_uses', 'offloadable_uses')
    local state, non_offloadable_uses, offloadable_uses = result[1], result[2], result[3]

    -- If session is not deletable, return an error.
    if non_offloadable_uses ~= "0" or offloadable_uses ~= "0" or state == 'OFFLOADING' or state == 'ONLOADING' then
        return redis.error_reply('[Ermes]: Session is not deletable')
    end

    -- TODO: Find the best default value for count. Note that we unpack the
    -- result of the scan for performance reasons, and that limits the maximum
    -- value of count.
    count = count or 100
    -- Delete "count" keys that starts with session from the session data.
    local match = session_data_key(session_id, '*')
    local result = redis.call('SCAN', 0, 'MATCH', match, 'COUNT', count)
    -- Unlink the keys.
    -- FIXME: This is a blocking operation, we should unlink the keys in
    -- batches.
    redis.call('UNLINK', table.unpack(result[2]))

    -- If there are no more keys to delete, delete the session metadata.
    if result[1] == '0' then
        -- Delete the session metadata.
        redis.call('DEL', metadata_key)
        -- Remove it from the offloadable_sessions_set.
        redis.call('ZREM', offloadable_sessions_set, session_id)
        -- Remove it from the sessions_set.
        redis.call('ZREM', sessions_set, session_id)
        -- Return 0 and the number of deleted keys.
        return { #result[2], 0 }
    end

    -- Otherwise, return 1 and the number of deleted keys (Note that scan may
    -- return more keys than count).
    return { #result[2], 1 }
end

-- Function that delete a session. It first delete "count" keys from the session data, then, if there are more keys to
-- delete, it returns 1, otherwise it deletes also the session metadata.
redis.register_function('delete_chunk', function(keys, args)
    -- Keys.
    local session_id = keys[1]
    -- Delete the session.
    return delete_session_chunk(session_id, 100)
end)

-- Function that delete all the sessions that are not used and are expired.
redis.register_function('garbage_collect', function(keys, args)
    -- Args.
    local ttlAfterExpiration = args[1]
    -- Count the deleted sessions.
    local deleted = 0
    local count = 100
    -- Remove count sessions data keys.
    repeat
        -- Retrieve one expired sessions to delete from the sessions_set.
        local expiredSession = redis.call('ZRANGEBYSCORE', sessions_set, '0', redis.call('TIME')[1], 'LIMIT', 0, 1)
        -- If there are no more expired sessions, return.
        if #expiredSession == 0 then
            expiredSession = redis.call('ZRANGEBYSCORE', sessions_set, '-inf',
                redis.call('TIME')[1] - tonumber(ttlAfterExpiration), 'LIMIT', 0, 1)

            if #expiredSession == 0 then
                return { 0, deleted }
            end
        end

        if deleted >= count then
            return { 1, deleted }
        end

        -- Delete the session.
        local flag, deletedKeys
        repeat
            -- Delete the session.
            deletedKeys, flag = delete_session_chunk(expiredSession[1])

            -- If there is an error, return it.
            if type(flag) == 'table' then
                return flag
            end

            -- Increment deleted.
            deleted = deleted + deletedKeys
        until flag == 0 or deleted >= count
    until true
end)

-- Function that create a node and register it.
redis.register_function('register_node', function(keys, args)
    -- Keys.
    local node_id = keys[1]
    -- Args.
    local jsonNode = args[1]
    local node = cjson.decode(jsonNode)
    local coords = node['geoCoordinates']

    -- Check if the node id is valid.
    assert_valid_id(node_id)
    -- Check if the node coordinates are valid.
    assert_valid_geo_coordinates(coords['latitude'], coords['longitude'])

    -- Add the node to the nodes_geoset.
    redis.call('GEOADD', nodes_geoset, tostring(coords['longitude'] + 0.0), tostring(coords['latitude'] + 0.0), node_id)

    -- Set the json at node_id.
    redis.call('SET', infrastructure_key(node_id), jsonNode)
    -- Return OK.
    return 'OK'
end)

-- Function that create a node and register it.
redis.register_function('register_node_relation', function(keys, args)
    -- Keys.
    local parent_node_id = keys[1]
    local child_node_id = keys[2]

    -- Set the parent-child relations.
    redis.call('SET', infrastructure_parent_key(child_node_id), parent_node_id)
    redis.call('SADD', infrastructure_children_key(parent_node_id), child_node_id)

    -- Return OK.
    return 'OK'
end)

-- Function that get the node by id.
redis.register_function('get_parent_node_of', function(keys, args)
    -- Keys.
    local node_id = keys[1]

    -- Get the parent.
    local parent = redis.call('GET', infrastructure_parent_key(node_id))

    -- If parent is nil, return nil.
    if parent == nil or parent == "" or parent == false then
        return ""
    end

    return redis.call('GET', infrastructure_key(parent))
end)

-- Function that get the node by id.
redis.register_function('get_children_nodes_of', function(keys, args)
    -- Keys.
    local node_id = keys[1]

    -- Get the children.
    local children = redis.call('SMEMBERS', infrastructure_children_key(node_id))

    if children == nil then
        return {}
    end

    -- Get the json of the children.
    local ArrayOfJsons = {}

    for _, child in ipairs(children) do
        table.insert(ArrayOfJsons, redis.call('GET', infrastructure_key(child)))
    end

    -- Return the json.
    return ArrayOfJsons
end)

redis.register_function('find_lookup_node', function(keys, args)
    -- Keys.
    local session_id = keys[1]

    -- Retrieve client location and created_in node.
    local metadata_key = session_metadata_key(session_id)
    local result = redis.call('HMGET', metadata_key, 'client_lat', 'client_long', 'created_in')
    local client_lat, client_long, created_in = result[1], result[2], result[3]

    -- If client location is not set, approximate them with the created_in node.
    if client_lat == "" or client_long == "" then
        -- If the created_in node has children, return it.
        local children = redis.call('SMEMBERS', infrastructure_children_key(created_in))

        if #children > 0 then
            return redis.call('GET', infrastructure_key(created_in))
        end

        -- Get the geo coords from the geo set.
        local coords = redis.call('GEOPOS', nodes_geoset, created_in)
        client_lat, client_long = coords[1][1], coords[1][2]
    end

    -- Get the closest node.
    local closest = redis.call('GEOSEARCH', nodes_geoset, 'FROMLONLAT', client_long, client_lat, 'BYRADIUS', '1000000',
        'KM',
        'ASC', 'COUNT', '20', 'WITHDIST')

    -- Loop until a node with children is found and return it.
    for _, node in ipairs(closest) do
        local children = redis.call('SMEMBERS', infrastructure_children_key(node[1]))
        if #children > 0 then
            return redis.call('GET', infrastructure_key(node[1]))
        end
    end

    return redis.error_reply('No node found')
end)
