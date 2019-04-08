require('os')
require('math')

function random()
    return string.format('{"event_type": "%s%d", "ts": %d, "params": {"p0":5, "p1":%d, "p2":%d} }',
        "test", math.random(1,10), os.time(), math.random(1,100), math.random(1,10))
end

request = function()
    wrk.headers["Content-Type"] = "application/json"
    return wrk.format("POST", wrk.path, wrk.headers, random())
end