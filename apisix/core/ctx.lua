--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
local log          = require("apisix.core.log")
local tablepool    = require("tablepool")
local get_var      = require("resty.ngxvar").fetch
local get_request  = require("resty.ngxvar").request
local ck           = require "resty.cookie"
local setmetatable = setmetatable
local ffi          = require("ffi")
local C            = ffi.C
local sub_str      = string.sub
local rawset       = rawset
local ngx_var      = ngx.var
local re_gsub      = ngx.re.gsub
local type         = type
local error        = error


ffi.cdef[[
int memcmp(const void *s1, const void *s2, size_t n);
]]


local _M = {version = 0.2}


do
    local var_methods = {
        -- ngx.req.get_method方法用于获取当前请求的method，如GET、POST
        method = ngx.req.get_method,
        cookie = function () return ck:new() end
    }

    local ngx_var_names = {
        upstream_scheme            = true,
        upstream_host              = true,
        upstream_upgrade           = true,
        upstream_connection        = true,
        upstream_uri               = true,

        upstream_mirror_host       = true,

        upstream_cache_zone        = true,
        upstream_cache_zone_info   = true,
        upstream_no_cache          = true,
        upstream_cache_key         = true,
        upstream_cache_bypass      = true,
        upstream_hdr_expires       = true,
        upstream_hdr_cache_control = true,
    }

    local mt = {
        -- 从t中获取指定的key属性
        __index = function(t, key)
            if type(key) ~= "string" then
                error("invalid argument, expect string value", 2)
            end

            local val
            -- 如果key是method或者cookie，则返回var_methods的对应属性的调用
            local method = var_methods[key]
            if method then
                val = method()
            -- 如果key以cookie_开头，即想要获取的是cookie的值
            elseif C.memcmp(key, "cookie_", 7) == 0 then
                local cookie = t.cookie
                if cookie then
                    local err
                    -- 获取cookie，这里以key的第8位开始的字符串作为key是因为nginx中获取cookie的key是cookie_{name}
                    val, err = cookie:get(sub_str(key, 8))
                    if not val then
                        log.warn("failed to fetch cookie value by key: ",
                                 key, " error: ", err)
                    end
                end
            -- 如果key以http_开头，即想要获取的是某个header的值，此时需要将key转换为小写并且将-转换为_，这是nginx要求的获取header值
            -- 的方式，http://nginx.org/en/docs/http/ngx_http_core_module.html#var_http_
            elseif C.memcmp(key, "http_", 5) == 0 then
                key = key:lower()
                key = re_gsub(key, "-", "_", "jo")
                -- get_var方法目前的实现是直接从ngx.var获取变量，传入的t._request参数没有被用到
                val = get_var(key, t._request)

            else
                -- 其他变量直接获取
                val = get_var(key, t._request)
            end

            if val ~= nil then
                -- 将val赋值给t的key属性，rawset方法就是忽略table对应的metatable，绕过metatable的行为约束，强制对原始表进行一次原
                -- 始的操作，也就是一次不考虑元表的简单更新
                rawset(t, key, val)
            end

            return val
        end,

        -- __newindex元方法用来对表更新
        __newindex = function(t, key, val)
            -- 如果当前的key是一个nginx变量，则保存到ngx.var中
            if ngx_var_names[key] then
                ngx_var[key] = val
            end

            -- log.info("key: ", key, " new val: ", val)
            -- 执行赋值操作
            rawset(t, key, val)
        end,
    }

-- 为ctx设置了var属性，同时为var属性设置了元表，对var获取属性时会对key做一些处理，同时会在ngx.var中找指定的key的值
function _M.set_vars_meta(ctx)
    local var = tablepool.fetch("ctx_var", 0, 32)
    var._request = get_request()
    setmetatable(var, mt)
    ctx.var = var
end

function _M.release_vars(ctx)
    if ctx.var == nil then
        return
    end

    tablepool.release("ctx_var", ctx.var)
    ctx.var = nil
end

end -- do


return _M
