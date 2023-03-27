dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local item_type = nil
local item_name = nil
local item_value = nil
local item_server_id = nil
local item_file_id = nil

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false

local discovered_items = {[""]={}}
local bad_items = {}
local ids = {}

local retry_url = false

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

find_item = function(url)
  if item_name then
    return nil
  end
  value2, value = string.match(url, "^https?://www([^%.]+)%.zippyshare%.com/v/([^/]+)/file%.html$")
  type_ = "file"
  if not value then
    value = string.match(url, "^https?://zippyshare%.com/([^/]+)")
    type_ = "user"
  end
  if value then
    item_type = type_
    if type_ == "file" then
      item_value = value2 .. ":" .. value
      item_server_id = value2
      item_file_id = value
    else
      item_value = value
    end
    item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      ids = {}
      ids[value] = true
      abortgrab = false
      tries = 0
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

discover_item = function(item)
  if not discovered_items[""][item] then
print(item)
    discovered_items[""][item] = true
  end
end

allowed = function(url, parenturl)
  local a, b = string.match(url, "^https?://www([0-9]+)%.zippyshare%.com/v/([^/]+)/")
  if a and b then
    discover_item("file:" .. a .. ":" .. b)
  end

  local a = string.match(url, "^https?://[^/]+zippyshare%.com/([^/%?&;]+)")
  if a then
    discover_item("user:" .. a)
  end

  for s in string.gmatch(url, "([0-9a-zA-Z%-_]+)") do
    if ids[s] then
      return true
    end 
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return unicode_codepoint_as_utf8(tonumber(s, 16))
      end
    )
    return newurl
  end

  local function check(newurl)
    newurl = decode_codepoint(newurl)
    local origurl = url
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function queue_tree(json)
    for k, v in pairs(json) do
      if type(v) == "table" then
        queue_tree(v)
      end
      if k == "ident" then
        check("https://zippyshare.com/rest/public/getTree?user=" .. json["user"] .. "&ident=" .. v .. "&id=%23")
        check("https://zippyshare.com/" .. json["user"] .. "/" .. v .. "/dir.html")
      end
    end
  end

  if (
      allowed(url)
      or (item_type == "user" and string.match(url, "/filetable%.jsp"))
    )
    and status_code < 300
    and (string.match(url, "^https?://[^/]+/v/") or item_type == "user") then
    html = read_file(file)
    if string.match(url, "^https?://[^/]+/v/") then
      local a, b = string.match(html, "document%.getElementById%('dlbutton'%)%.omg%s*=%s*([0-9]+)%%([0-9]+);")
      if not a
        and (
          string.match(html, ">File has expired and does not exist anymore on this server<")
          or string.match(html, ">File does not exist on this server<")
        ) then
        io.stdout:write("This file is deleted.\n")
        io.stdout:flush()
        return urls
      end
      local c, d = string.match(html, "var%s+b%s*=%s*parseInt%(document%.getElementById%('dlbutton'%)%.omg%)%s*%*%s*%(([0-9]+)%%([0-9]+)%);")
      local e = string.match(html, "document%.getElementById%('dlbutton'%)%.href%s*=%s*\"/d/[^/\"]+/\"%+%(b%+([0-9]+)%)%+\"")
      local value = (a % b) * (c % d) + e
      html = string.gsub(html, '/"%+%(b%+' .. e .. '%)%+"/', "/" .. tostring(value) .. "/")
      for s in string.gmatch(html, "setLocale%('([a-z]+)'%);") do
        check("https://www16.zippyshare.com/view.jsp?locale=" .. s .. "&key=" .. item_file_id)
      end
    end
    if string.match(url, "/dir%.html") then
      local vars_data = string.match(html, "ZippyFileManager%s*=%s*{(.-)}")
      local dir_id = string.match(vars_data, "dir:%s*'([^']+)'")
      local user = string.match(vars_data, "user:%s*'([^']+)'")
      check("https://zippyshare.com/rest/public/getTree?user=" .. user .. "&ident=" .. dir_id .. "&id=%23")
      check("https://zippyshare.com/wojo/" .. dir_id .. "/dir.html")
      local partial_data = "user=" .. user
        .. "&dir=" .. dir_id
        .. "&sort=" .. string.match(vars_data, "sortcol:%s*'([^']+)'") .. string.match(vars_data, "sortdir:%s*'([^']+)'")
        .. "&pageSize=" .. string.match(vars_data, "pageSize:%s*([0-9]+)")
        .. "&search=" .. string.match(vars_data, "searchStr:%s*'([^']*)'")
        .. "&viewType=" .. string.match(vars_data, "viewType:%s*'([^']+)'")
      local max_page = 0
      for page_num in string.gmatch(html, "ZippyFileManager%.browsePage%(([0-9]+)%)") do
        page_num = tonumber(page_num)
        if page_num > max_page then
          max_page = page_num
        end
      end
      for page_num=0,max_page do
        page_num = tostring(page_num)
        local data = "page=" .. page_num .. "&" .. partial_data
        if not addedtolist[data] then
print(data)
          table.insert(urls, {
            url="https://zippyshare.com/fragments/publicDir/filetable.jsp",
            method="POST",
            body_data=data
          })
          addedtolist[data] = true
        end
      end
    end
    if string.match(url, "/rest/public/getTree") then
      local json = JSON:decode(html)
      queue_tree(json)
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()

  if killgrab then
    return wget.actions.ABORT
  end

  find_item(url["url"])

  if abortgrab then
    abort_item()
    return wget.actions.ABORT
  end

  if status_code == 301
    and string.match(url["url"], "^https?://[^/]+/view%.jsp") then
    return wget.actions.EXIT
  end

  if status_code == 0 or status_code ~= 200 then
    io.stdout:write("Server returned bad response. Sleeping.\n")
    io.stdout:flush()
    local maxtries = 3
    tries = tries + 1
    if tries > maxtries then
      tries = 0
      abort_item()
      return wget.actions.ABORT
    end
    os.execute("sleep " .. math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    ))
    return wget.actions.CONTINUE
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key, shard)
    local tries = 0
    local maxtries = 10
    local parameters = ""
    if shard ~= "" then
      parameters = "?shard=" .. shard
    end
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key .. parameters,
        items .. "\0"
      )
      if code == 200 and body ~= nil and JSON:decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        break
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == maxtries then
      kill_grab()
    end
  end

  for key, data in pairs({
    ["zippyshare-46gn9f2zv7v2fjhk"] = discovered_items
  }) do
    for shard, urls_data in pairs(data) do
      print('queuing for', string.match(key, "^(.+)%-"), "on shard", shard)
      local items = nil
      local count = 0
      local progress_count = 0
      local all_counted = 0
      for _ in pairs(urls_data) do
        all_counted = all_counted + 1
      end
      print("queuing", all_counted, " items")
      for item, _ in pairs(urls_data) do
        print("found item", item)
        if items == nil then
          items = item
        else
          items = items .. "\0" .. item
        end
        count = count + 1
        progress_count = progress_count + 1
        if count == 400 then
          io.stdout:write(tostring(progress_count) .. " of " .. tostring(all_counted) .. " ")
          submit_backfeed(items, key, shard)
          items = nil
          count = 0
        end
      end
      if items ~= nil then
        submit_backfeed(items, key, shard)
      end
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
    return wget.exits.IO_FAIL
  end
  return exit_status
end

