local counter = 1

-- Function to generate a zero-padded string
local function pad(num)
    return string.format("%03d", num)
end

-- Initialize the request
request = function()
    counter = counter + 1
    local value = pad(counter)
    local request_id = pad(counter)
    local path = string.format("/?topic=test_topic1&value=%s&request_id=%s", value, request_id)
    print(path)
    return wrk.format("GET", path)
end

-- Optional: You can add response() function to track responses
response = function(status, headers, body)
    print(status)
    print(body)
    -- You can add logic here to track responses if needed
    -- For example:
    if status ~= 200 then
        print(status)
        print(headers)
        -- Track successful responses
    end
end

-- wrk -t1 -c200 -d20s -R5000 -s bench.lua --latency http://3.76.221.107:8080 bench.lua