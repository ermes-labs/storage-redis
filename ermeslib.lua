#!lua name=ermeslib

--[[
The states of a single session are the following:
    - ONLOADING     : The session is being onloaded from another node.
    - ACTIVE        : The session is active.
    - OFFLOADING    : The session is being offloaded to another node.
    - OFFLOADED     : The session has been offloaded to another node, it will remain so for a while to allow the node
                      to notify the offload to the client on the following requests.
For internal functioning, in each state we track
    - ReadWriteUses : The number of read-write uses of the session.
    - ReadOnlyUses  : The number of read-only uses of the session.
    - used          : If the session is being used or not. We track sessions in ordered sets to know which sessions can
                      be offloaded (not used) and which is expired to allow garbage collection. Note that a session can
                      expire while it is being used, it won't be erased until it is not used anymore.

The full state machine is described here:
    ONLOADING: The session is being onloaded from another node.
        - State:
            - ReadWriteUses : 0,
            - ReadOnlyUses  : 0,
            - Used          : true
        - Transitions:
            (_, _) onload_data   -> ONLOADING (_, _)}.
            (_, _) onload_finish -> ACTIVE    (_, _)}.

    ACTIVE: The session is active
        - State
            - ReadWriteUses : 0-N,
            - ReadOnlyUses  : 0-N,
            - Used          : false if ReadWriteUses == 0 and ReadOnlyUses == 0, true otherwise
        - Transitions:
            (0-N, _  ) rw-acquire   -> ACTIVE     ($++, _  )}.
            (1-N, _  ) rw-release   -> ACTIVE     ($--, _  )}.
            (_  , 0-N) ro-acquire   -> ACTIVE     (_  , $++)}.
            (_  , 1-N) ro-release   -> ACTIVE     (_  , $--)}.
            (0  , 0-N) offload      -> OFFLOADING (_  , _  )}.

    OFFLOADING: The session is being offloaded to another node.
        - State
            - ReadWriteUses : 0,
            - ReadOnlyUses  : 0-N
            - Used          : true
        - Transitions
            (_  , 0-N) ro-acquire       -> ACTIVE    (_  , $++)}.
            (_  , 1-N) ro-release       -> ACTIVE    (_  , $--)}.
            (_  , _  ) offload-finish   -> OFFLOADED (_  , _  )}.
            (_  , _  ) offload-cancel   -> ACTIVE    (_  , _  )}. + restore expiration
      }

    OFFLOADED: The session has been offloaded to another node.
        - State
            - ReadWriteUses : 0,
            - ReadOnlyUses  : 0-N
            - Used          : true if ReadOnlyUses > 0, false otherwise
        - Transitions
            (_  , 1-N) ro-release       -> ACTIVE    (_  , $--)}.
--]]

-- Generate a key in the infrastructure keyspace.
local function infrastructureKey(key)
    return 'i:' .. key
end
-- Generate a key in the config keyspace.
local function configKey(key)
    return 'c:' .. key
end
-- Generate a key in the session keyspace.
local function sessionDataKey(sessionId, key)
    -- sessionId must be 36 bytes long
    if #sessionId ~= 36 then
        return redis.error_reply('[Ermes]: Session must be 36 bytes long')
    end

    return 's:' .. sessionId .. ':' .. key
end
-- Generate a key in the session metadata keyspace.
local function sessionMetadataKey(sessionId)
    -- sessionId must be 36 bytes long
    if #sessionId ~= 36 then
        return redis.error_reply('[Ermes]: Session must be 36 bytes long')
    end

    --[[
    The session metadata is stored in a hash with the following fields (Note that some properties are like score and
    expiration are stored also in the sorted sets and are kept in sync):
        'state',
        'rwUses',
        'roUses',
        'clientX',
        'clientY',
        'offloadedToGateway',
        'offloadedToSession',
        '_created_in',
        '_created_at',
        '_created_at',
        '_expires_at',
        '_updated_at',
        '_activity_score',
    --]]
    return 'm:' .. sessionId .. ':data'
end

-- Ordered set by expiration (or +inf if no expiration is set) of the sessions that are active.
local usedSessionsSet = configKey('usedSessionsSet')
-- Ordered set by expiration (or +inf if no expiration is set) of the sessions that are not active. Those sessions are
-- eligible for garbage collection.
local unusedSessionsSet = configKey('unusedSessionsSet')
-- Ordered set by score of the sessions that can be offloaded.
local offloadableSessionsSet = configKey('offloadableSessionsSet')
-- Geo set of the nodes that are tracked.
local trackedNodesGeoSet = configKey('trackedNodesGeoSet')
-- Key mapped to the id of the current node.
local currentNodeIdKey = configKey('currentNode')

-- Compute the score of a session. The higher the score, the more the session is 
-- a good candidate for offloading.
local function update_score(smKey)
    -- Get the session metadata attributes.
    local _updated_at, _activity_score = redis.call('HMGET', smKey, '_updated_at', '_activity_score')
    local currentTime = redis.call('TIME')[1]
    -- Parse.
    _updated_at = tonumber(_updated_at)
    _activity_score = tonumber(_activity_score or 0)

    -- Constants.
    local decay = 0.1
    local deltaTime = currentTime - _updated_at
    -- The score starts from the _updated_at field.
    local _activity_score = currentTime + _activity_score * math.exp(-decay * deltaTime)
    -- If the session was created in another node.
    redis.call('HMSET', smKey, '_activity_score', _activity_score)

    -- TODO: Add geo distance to the score.
    return -_activity_score
end

-- Compute the score of a session. The higher the score, the more the session is 
-- a good candidate for offloading.
local function compute_score(currentTime, _updated_at, _activity_score)
    -- Constants.
    local decay = 0.1
    local deltaTime = currentTime - _updated_at
    -- The score starts from the _updated_at field.
    local _activity_score = currentTime + _activity_score * math.exp(-decay * deltaTime)
    -- Return the score.
    return {_activity_score, -_activity_score}
end

-- Function that set the expire time
local function set_expire_time(sessionId, _expires_at)
    local smKey = sessionMetadataKey(sessionId)
    -- Set the session metadata attributes.
    redis.call('HMSET', smKey,
            '_expires_at', _expires_at,
            '_updated_at', redis.call('TIME')[1])

    -- Check if the session is being used or not.
    if redis.call('ZSCORE', unusedSessionsSet, sessionId) ~= nil then
        -- Add it to the unusedSessionsSet.
        redis.call('ZADD', unusedSessionsSet, _expires_at, sessionId)
    else
        -- Add it to the usedSessionsSet.
        redis.call('ZADD', usedSessionsSet, _expires_at, sessionId)
    end

    -- Return OK.
    return 'OK'
end

local function delete_session_chunk(sessionId)
    -- Get the current state.
    local smKey = sessionMetadataKey(sessionId)
    local state, rwUses, roUses = redis.call('HMGET', smKey, 'state', 'rwUses', 'roUses')

    -- If session is not deletable, return an error.
    if rwUses ~= 0 or roUses ~= 0 or state == 'OFFLOADING' or state == 'ONLOADING' then
        return redis.error_reply('[Ermes]: Session is not deletable')
    end

    -- TODO: Find the best default value for count. Note that we unpack the 
    -- result of the scan for performance reasons, and that limits the maximum 
    -- value of count.
    local count = 100
    -- Delete "count" keys that starts with session from the session data.
    local match = sessionDataKey(sessionId, '*')
    local result = redis.call('SCAN', 0, 'MATCH', match, 'COUNT', count)
    -- Unlink the keys.
    redis.call('UNLINK', unpack(result[2]))

    -- If there are no more keys to delete, delete the session metadata.
    if result[1] == '0' then
        -- Delete the session metadata.
        redis.call('DEL', smKey)
        -- Remove it from the offloadableSessionsSet.
        redis.call('ZREM', offloadableSessionsSet, sessionId)
        -- Remove it from the usedSessionsSet.
        redis.call('ZREM', usedSessionsSet, sessionId)
        -- Remove it from the unusedSessionsSet.
        redis.call('ZREM', unusedSessionsSet, sessionId)
        -- Return 0 and the number of deleted keys.
        return {0, #result[2]}
    end

    -- Otherwise, return 1 and the number of deleted keys (Note that scan may 
    -- return more keys than count).
    return {1, #result[2]}
end

-- Function that create a session and acquire it in read-write mode.
redis.register_function('create_and_rw_acquire', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Args.
    local clientX = tonumber(args[1])
    local clientY = tonumber(args[2])
    local _expires_at = tonumber(args[3])
    -- Compute the session metadata key.
    local smKey = sessionMetadataKey(sessionId)
    local _created_in = redis.call('GET', currentNodeIdKey)

    -- If session already exists, return an error.
    if redis.call('EXISTS', smKey) == 1 then
        return false
    end

    -- If clientX or clientY are not provided, aproximate them with the
    -- coordinates of the node that created the session.
    if clientX == nil or clientY == nil then
        clientX, clientY = redis.call('GEOPOS', trackedNodesGeoSet, _created_in)[1]
        clientX = tonumber(clientX)
        clientY = tonumber(clientY)
    end

    -- Get the current time.
    local time = redis.call('TIME')[1]
    -- Set the session metadata attributes.
    redis.call('HMSET', smKey,
            'state', 'ACTIVE',
            'rwUses', 1,
            'roUses', 0,
            'clientX', clientX,
            'clientY', clientY,
            '_created_in', _created_in,
            '_created_at', time,
            '_updated_at', time,
            '_expires_at', _expires_at)

    -- Add it to the unusedSessionsSet.
    redis.call('ZADD', usedSessionsSet, _expires_at or '+inf', sessionId)

    -- Return true.
    return true
end)

-- Function that create a session and acquire it in read-only mode.
redis.register_function('create_and_ro_acquire', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Args.
    local clientX = tonumber(args[1])
    local clientY = tonumber(args[2])
    local _expires_at = tonumber(args[3])
    -- Compute the session metadata key.
    local smKey = sessionMetadataKey(sessionId)
    local _created_in = redis.call('GET', currentNodeIdKey)

    -- If session already exists, return an error.
    if redis.call('EXISTS', smKey) == 1 then
        return false
    end

    -- If clientX or clientY are not provided, aproximate them with the
    -- coordinates of the node that created the session.
    if clientX == nil or clientY == nil then
        clientX, clientY = redis.call('GEOPOS', trackedNodesGeoSet, _created_in)[1]
        clientX = tonumber(clientX)
        clientY = tonumber(clientY)
    end

    -- Get the current time.
    local time = redis.call('TIME')[1]
    -- Set the session metadata attributes.
    redis.call('HMSET', smKey,
            'state', 'ACTIVE',
            'rwUses', 0,
            'roUses', 1,
            'clientX', clientX,
            'clientY', clientY,
            '_created_in', _created_in,
            '_created_at', time,
            '_updated_at', time,
            '_expires_at', _expires_at)

    -- Add it to the unusedSessionsSet.
    redis.call('ZADD', usedSessionsSet, _expires_at or '+inf', sessionId)

    -- Return true.
    return true
end)

-- Function that create a session and set it for onload.
redis.register_function('onload_start', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Args.
    local clientX = tonumber(args[1])
    local clientY = tonumber(args[2])
    local _created_in = args[3]
    local _created_at = tonumber(args[4])
    local _updated_at = tonumber(args[5])
    local _expires_at = tonumber(args[6])
    -- Compute the session metadata key.
    local smKey = sessionMetadataKey(sessionId)

    -- If session already exists, return false.
    if redis.call('EXISTS', smKey) == 1 then
        return false
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', smKey,
            'state', 'ONLOADING',
            'rwUses', 0,
            'roUses', 0,
            'clientX', clientX,
            'clientY', clientY,
            '_created_in', _created_in,
            '_created_at', _created_at,
            '_updated_at', _updated_at,
            '_expires_at', _expires_at)

    -- Add it to the unusedSessionsSet.
    redis.call('ZADD', usedSessionsSet, _expires_at or '+inf', sessionId)

    -- Return true.
    return true
end)

-- Function that set the session data after onload.
-- TODO: This function should onload session data in batches, to avoid blocking
-- the redis instance for too long.
redis.register_function('onload_data', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Args.
    local data = cjson.decode(args[1])
    -- Get the current state.
    local smKey = sessionMetadataKey(sessionId)
    local state = redis.call('HMGET', smKey, 'state')

    -- If session is not ONLOADING, return an error.
    if state ~= 'ONLOADING' then
        return redis.error_reply('[Ermes]: Session is not ONLOADING')
    end

    local strings = data['strings']
    -- Set the session data.
    for key, value in pairs(strings) do
        redis.call('SET', sessionDataKey(sessionId, key), value)
    end

    local lists = data['lists']
    -- Set the session data.
    for key, value in pairs(lists) do
        redis.call('RPUSH', sessionDataKey(sessionId, key), unpack(value))
    end

    local sets = data['sets']
    -- Set the session data.
    for key, value in pairs(sets) do
        redis.call('SADD', sessionDataKey(sessionId, key), unpack(value))
    end

    local sortedSets = data['sortedSets']
    -- Set the session data.
    for key, value in pairs(sortedSets) do
        redis.call('ZADD', sessionDataKey(sessionId, key), unpack(value))
    end

    local hashes = data['hashes']
    -- Set the session data.
    for key, value in pairs(hashes) do
        redis.call('HMSET', sessionDataKey(sessionId, key), unpack(value))
    end

    -- Return OK.
    return 'OK'
end)

-- Function that set the session as active after onload.
redis.register_function('onload_finish', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Get the current state.
    local smKey = sessionMetadataKey(sessionId)
    local state, _expires_at, _updated_at, _activity_score = redis.call('HMGET', smKey, 'state', '_expires_at', '_updated_at', '_activity_score')
    local currentTime = redis.call('TIME')[1]
    local _score = compute_score(smKey)

    -- If session is not ONLOADING, return an error.
    if state ~= 'ONLOADING' then
        return redis.error_reply('[Ermes]: Session is not ONLOADING')
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', smKey,
            'state', 'ACTIVE',
            '_activity_score', _score,
            '_score', _score)

    -- Add it to the offloadableSessionsSet.
    redis.call('ZADD', offloadableSessionsSet, _score, sessionId)
    -- Remove it from the unusedSessionsSet.
    redis.call('ZREM', usedSessionsSet, sessionId)
    -- Add it to the usedSessionsSet.
    redis.call('ZADD', unusedSessionsSet, _expires_at or '+inf', sessionId)

    -- Return OK.
    return 'OK'
end)

-- Function that acquire a session in read-write mode.
redis.register_function('rw_acquire', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Get the current state.
    local smKey = sessionMetadataKey(sessionId)
    local time = redis.call('TIME')[1]
    local state, rwUses, roUses, offloadedToGateway, offloadedToSession, _expires_at = redis.call('HMGET', smKey, 'state', 'rwUses', 'roUses', 'offloadedToGateway', 'offloadedToSession', '_expires_at')

    -- If session is OFFLOADED, return the state of the session and the offloadedTo data.
    if state == 'OFFLOADED' then
        return {state, offloadedToGateway, offloadedToSession}
    end

    -- If session is not ACTIVE or is expired, return an error.
    if state ~= 'ACTIVE' or (_expires_at ~= nil and _expires_at < time) then
        return redis.error_reply('[Ermes]: Session is not ACTIVE or is expired')
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', smKey,
            'rwUses', rwUses + 1,
            '_updated_at', time)

    if rwUses == 0 then
        -- Remove it from the offloadableSessionsSet.
        redis.call('ZREM', offloadableSessionsSet, sessionId)

        if roUses == 0 then
            -- Remove it from the unusedSessionsSet.
            redis.call('ZREM', unusedSessionsSet, sessionId)
            -- Add it to the usedSessionsSet.
            redis.call('ZADD', usedSessionsSet, _expires_at or '+inf', sessionId)
        end
    end

    -- Return OK.
    return {state, _expires_at}
end)

-- Function that acquire a session in read-only mode.
redis.register_function('ro_acquire', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Get the current state.
    local smKey = sessionMetadataKey(sessionId)
    local time = redis.call('TIME')[1]
    local state, rwUses, roUses, offloadedToGateway, offloadedToSession, _expires_at = redis.call('HMGET', session, 'state', 'rwUses', 'roUses', 'offloadedToGateway', 'offloadedToSession', '_expires_at')

    -- If session is OFFLOADED, return the state of the session and the offloadedTo data.
    if state == 'OFFLOADED' then
        return {state, offloadedToGateway, offloadedToSession}
    end

    -- If session is not ACTIVE or is expired, return an error.
    if (state ~= 'ACTIVE' and state ~= 'OFFLOADING') or (_expires_at ~= nil and _expires_at < time) then
        return redis.error_reply('[Ermes]: Session is not ACTIVE or is expired')
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', smKey,
            'roUses', roUses + 1,
            '_updated_at', time)

    if rwUses == 0 and roUses == 0 then
        -- Remove it from the unusedSessionsSet.
        redis.call('ZREM', unusedSessionsSet, sessionId)
        -- Add it to the usedSessionsSet.
        redis.call('ZADD', usedSessionsSet, _expires_at or '+inf', sessionId)
    end

    -- Return OK.
    return {state, _expires_at}
end)

-- Function that release a previously acquired session in read-write mode.
redis.register_function('rw_release', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Get the current state.
    local smKey = sessionMetadataKey(sessionId)
    local time = redis.call('TIME')[1]
    local rwUses, roUses, _expires_at = redis.call('HMGET', smKey, 'rwUses', 'roUses', '_expires_at')
    local score = compute_score(smKey)

    -- If there are no rwUses, return an error.
    if rwUses == 0 then
        return redis.error_reply('[Ermes]: No rwUses to release')
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', smKey,
            'rwUses', rwUses - 1,
            '_score', score,
            '_updated_at', time)

    if rwUses == 1 then
        -- Add it to the offloadableSessionsSet.
        redis.call('ZADD', offloadableSessionsSet, score, sessionId)

        if roUses == 0 then
            -- Remove it from the usedSessionsSet.
            redis.call('ZREM', usedSessionsSet, sessionid)
            -- Add it to the unusedSessionsSet.
            redis.call('ZADD', unusedSessionsSet, _expires_at or '+inf', sessionId)
        end
    end

    -- Return OK.
    return 'OK'
end)

-- Function that release a previously acquired session in read-only mode.
redis.register_function('ro_release', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Get the current state.
    local smKey = sessionMetadataKey(sessionId)
    local time = redis.call('TIME')[1]
    local state, rwUses, roUses, _expires_at = redis.call('HMGET', smKey, 'state', 'rwUses', 'roUses', '_expires_at')
    local score = compute_score(smKey)

    -- If there are no roUses, return an error.
    if roUses == 0 then
        return redis.error_reply('[Ermes]: No roUses to release')
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', smKey,
            'roUses', roUses - 1,
            '_score', score,
            '_updated_at', time)

    if rwUses == 0 and roUses == 1 and state ~= 'OFFLOADING' then
        -- Remove it from the unusedSessionsSet.
        redis.call('ZREM', usedSessionsSet, sessionId)
        -- Add it to the usedSessionsSet.
        redis.call('ZADD', unusedSessionsSet, _expires_at or '+inf', sessionId)
    end

    -- Return OK.
    return 'OK'
end)

-- Function that start the offload of a session.
redis.register_function('offload_start', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Get the current state.
    local smKey = sessionMetadataKey(sessionId)
    local state, rwUses, _expires_at = redis.call('HMGET', smKey, 'state', 'rwUses', 'roUses', '_expires_at')
    local time = redis.call('TIME')[1]

    -- If session is not ACTIVE or has rwUses or is expired, return an error.
    if state ~= 'ACTIVE' or rwUses ~= 0 or (_expires_at ~= nil and _expires_at < time) then
        return redis.error_reply('[Ermes]: Session is not ACTIVE, has rwUses or is expired')
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', smKey,
            'state', 'OFFLOADING')

    -- Remove it from the offloadableSessionsSet.
    redis.call('ZREM', offloadableSessionsSet, sessionId)
    -- Remove it from the usedSessionsSet.
    redis.call('ZREM', unusedSessionsSet, sessionId)
    -- Add it to the unusedSessionsSet.
    redis.call('ZADD', usedSessionsSet, _expires_at or '+inf', sessionId)

    -- Return OK.
    return 'OK'
end)

-- Function that offload the data of a session.
redis.register_function('offload_data', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Get the current state.
    local smKey = sessionMetadataKey(sessionId)
    local state, rwUses, _expires_at = redis.call('HMGET', smKey, 'state', 'rwUses', '_expires_at')

    -- If session is not OFFLOADING, return an error.
    if state ~= 'OFFLOADING' then
        return redis.error_reply('[Ermes]: Session is not OFFLOADING')
    end

    -- Get the session data.
    local data = {
        strings = {},
        lists = {},
        sets = {},
        sortedSets = {},
        hashes = {}
    }
    
    -- Get the session data.
    local keys = redis.call('KEYS', sessionDataKey(sessionId, '*'))
    for _, key in ipairs(keys) do
        local keyType = redis.call('TYPE', key)['ok']
        if keyType == 'string' then
            data['strings'][key] = redis.call('GET', key)
        elseif keyType == 'list' then
            data['lists'][key] = redis.call('LRANGE', key, 0, -1)
        elseif keyType == 'set' then
            data['sets'][key] = redis.call('SMEMBERS', key)
        elseif keyType == 'zset' then
            data['sortedSets'][key] = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
        elseif keyType == 'hash' then
            data['hashes'][key] = redis.call('HGETALL', key)
        end
    end

    -- Return OK.
    return cjson.encode(data)
end)

-- Function that finish the offload of a session.
redis.register_function('offload_finish', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Args.
    local offloadedToGateway = args[1]
    local offloadedToSession = args[2]
    -- Get the current state.
    local smKey = sessionMetadataKey(sessionId)
    local state, roUses, _expires_at = redis.call('HMGET', smKey, 'state', 'roUses', '_expires_at')

    -- If session is not OFFLOADING, return an error.
    if state ~= 'OFFLOADING' then
        return redis.error_reply('[Ermes]: Session is not OFFLOADING')
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', smKey,
            'state', 'OFFLOADED',
            'offloadedToGateway', offloadedToGateway,
            'offloadedToSession', offloadedToSession,
            '_updated_at', redis.call('TIME')[1])

    if roUses == 0 then
        -- Remove it from the usedSessionsSet.
        redis.call('ZREM', usedSessionsSet, sessionId)
        -- Add it to the unusedSessionsSet.
        redis.call('ZADD', unusedSessionsSet, _expires_at or '+inf', sessionId)
    end

    -- Return OK.
    return 'OK'
end)

-- Function that cancel the offload of a session.
redis.register_function('offload_cancel', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Get the current state.
    local smKey = sessionMetadataKey(sessionId)
    local state, rwUses, _score, _expires_at = redis.call('HMGET', smKey, 'state', 'rwUses', '_score', '_expires_at')

    -- If session is not OFFLOADING, return an error.
    if state ~= 'OFFLOADING' then
        return redis.error_reply('[Ermes]: Session is not OFFLOADING')
    end

    -- Set the session metadata attributes.
    redis.call('HMSET', smKey, 
        'state', 'ACTIVE')

    -- Add it to the offloadableSessionsSet.
    redis.call('ZADD', offloadableSessionsSet, _score, sessionId)

    if rwUses == 0 then
        -- Remove it from the usedSessionsSet.
        redis.call('ZREM', usedSessionsSet, sessionId)
        -- Add it to the unusedSessionsSet.
        redis.call('ZADD', unusedSessionsSet, _expires_at or '+inf', sessionId)
    end

    -- Return OK.
    return 'OK'
end)

-- Function that delete a session. It first delete "count" keys from the session data, then, if there are more keys to
-- delete, it returns 1, otherwise it deletes also the session metadata.
redis.register_function('delete_chunk', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Delete the session.
    return delete_session_chunk(sessionId)
end)

-- Function that set the expiration time of a session.
redis.register_function('set_expire_time', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Args.
    local _expires_at = args[1]
    -- Set the expire time.
    return set_expire_time(sessionId, _expires_at)
end)

-- Function that set the session as expired.
redis.register_function('expire', function(keys, args)
    -- Keys.
    local sessionId = keys[1]
    -- Set the expire time to now.
    return set_expire_time(sessionId, redis.call('TIME')[1])
end)

-- Function that set the coordinates of the client of a session.
redis.register_function('set_client_coordinates', function(keys, args)
    -- Keys.
    local session = sessionMetadataKey(keys[1])
    -- Args.
    local clientX = args[1]
    local clientY = args[2]

    redis.call('HMSET', session,
            'clientX', clientX,
            'clientY', clientY,
            '_score', _score,
            '_updated_at', redis.call('TIME')[1])

    -- Compute the score of the session.
    local _score = compute_score(session)

    -- Update the score of the session.
    redis.call('ZADD', offloadableSessionsSet, _score, session)

    -- Return OK.
    return 'OK'
end)

-- Function that delete all the sessions that are not used and are expired.
redis.register_function('collect_expired', function(keys, args)
    -- Count the deleted sessions.
    local deleted = 0
    local count = 100
    -- Remove count sessions data keys.
    repeat 
        -- Retrieve one expired sessions to delete from the unusedSessionsSet.
        local expiredSession = redis.call('ZRANGEBYSCORE', unusedSessionsSet, '-inf', redis.call('TIME')[1], 'LIMIT', 0, 1)
        -- If there are no more expired sessions, return.
        if #expiredSession == 0 then
            return {0, deleted}
        end

        if deleted >= count then
            return {1, deleted}
        end

        -- Delete the session.
        local flag, deletedKeys
        repeat
            -- Delete the session.
            flag, deletedKeys = delete_session_chunk(expiredSession[1])

            -- If there is an error, return it.
            if type(flag) == 'table' then
                return flag
            end

            -- Increment deleted.
            deleted = deleted + deletedKeys
        until flag == 0 or deleted >= count

    until true
end)

-- Function that delete all the sessions that are not used and are expired.
redis.register_function('collect_expired_but_never_released', function(keys, args)
    -- Args.
    local ttlAfterExpiration = args[1]
    local deleted = 0
    local count = 100
    -- Remove count sessions data keys.
    repeat 
        -- Retrieve one expired sessions to delete from the usedSessionsSet.
        local expiredSession = redis.call('ZRANGEBYSCORE', usedSessionsSet, '-inf', redis.call('TIME')[1] - ttlAfterExpiration, 'LIMIT', 0, 1)
        -- If there are no more expired sessions, return.
        if #expiredSession == 0 then
            return count
        end

        if deleted >= count then
            return deleted
        end

        -- Delete the session.
        local flag, deletedKeys
        repeat
            -- Delete the session.
            flag, deletedKeys = delete_session_chunk(expiredSession[1])

            -- If there is an error, return it.
            if type(flag) == 'table' then
                return flag
            end

            -- Increment deleted.
            deleted = deleted + deletedKeys
        until flag == 0 or deleted >= count
    until true
end)
