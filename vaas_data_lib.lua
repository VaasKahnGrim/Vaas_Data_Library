local PLAYER = FindMetaTable'Player'

local msg = MsgC


--[[
	binarytable can be found here: https://github.com/darkjacky/gm_binarytable
]]
if(file.Exists('bin/gmsv_binarytable_linux.dll','LUA')&& system.IsLinux())||(file.Exists('bin/gmsv_binarytable_win32.dll','LUA')&& system.IsWindows())then
	require'binarytable'

	--Lua is a piece of shit with globals
	local TableToBinary = TableToBinary
	local BinaryToTable = BinaryToTable
	local file_CreatDir = file.CreateDir
	local file_Write = file.Write
	local file_Read = file.Read
	
	--Initial folder for binary storage
	file_CreateDir'binary_storage'

	--[[
		Files are saved reletive to the players SteamID64
	]]
	function PLAYER:SavePlayerTable(file_name,tbl)
		local Table = tbl --Don't undo this, this is because trying to save a global table is prone to crashing. This will prevent that issue(hopefully).
		local steamid = self:SteamID64()
		if file_Exists('binary_storage/'..steamid..'/'..file_name..'.dat','DATA')then
			file_CreateDir('binary_storage/'..steamid)
			file_Write('binary_storage/'..steamid..'/'..file_name..'.dat',TableToBinary(Table))
		else
			file_Write('binary_storage/'..steamid..'/'..file_name..'.dat',TableToBinary(Table))
		end
	end

	--[[
		Files are saved reletive to the players SteamID64
		Note: If the file does not exist then it will return an empty table
	]]
	function PLAYER:LoadPlayerTable(file_name)
		return BinaryToTable(file_Read('binary_storage/'..self:SteamID64()..'/'..file_name..'.dat','DATA')) || {}
	end
else
	msg(Color(255,0,0),'gmsv_binarytable was not located on the server. So we are opting for legacy versions of the table functions.\n')
	msg(Color(255,0,0),'They will function much slower, so be sure to go grab this module: https://github.com/darkjacky/gm_binarytable/releases\n')
	
	--Lua is a piece of shit with globals
	local util_JSONToTable = util.JSONToTable
	local util_TableToJSON = util.TableToJSON
	local file_CreatDir = file.CreateDir
	local file_Write = file.Write
	local file_Read = file.Read

	--Initial folder for raw storage(we keep this seperated because of binary_storage being meant to be smaller and is a complete differant format)
	file_CreateDir'raw_storage'

	--[[
		Files are saved reletive to the players SteamID64
	]]
	function PLAYER:SavePlayerTable(file_name,tbl)
		local steamid = self:SteamID64()
		if file_Exists('raw_storage/'..steamid..'/'..file_name..'.dat','DATA')then
			file_CreateDir('raw_storage/'..steamid)
			file_Write('raw_storage/'..steamid..'/'..file_name..'.dat',util_TableToJSON(tbl))
		else
			file_Write('raw_storage/'..steamid..'/'..file_name..'.dat',util_TableToJSON(tbl))
		end
	end
	
	--[[
		Files are saved reletive to the players SteamID64
		Note: If the file does not exist then it will return an empty table
	]]
	function PLAYER:LoadPlayerTable(file_name)
		return util_JSONToTable(file_Read('raw_storage/'..self:SteamID64()..'/'..file_name..'.dat','DATA')) || {}
	end
end

--[[
	We prioritize tmysql4 as it has far less memory issues, you should stop using mysqloo btw. Its a pos.

	tmysql4 can be found here: https://github.com/SuperiorServers/gm_tmysql4
	mysqloo can be found here: https://github.com/FredyH/MySQLOO
]]
if(file.Exists('bin/gmsv_tmysql4_linux.dll','LUA')&& system.IsLinux())||(file.Exists('bin/gmsv_tmysql4_win32.dll','LUA')&& system.IsWindows())then
	--[[
		This section is taken from the Dash library due to being well built for handling tmysql4 stuff.
		https://github.com/SuperiorServers/dash/blob/master/lua/dash/libraries/server/mysql.lua
	]]

	require'tmysql4'

	mysql = setmetatable({
		GetTable = setmetatable({},{
			__call = function(self)
				return self
			end
		})
	},{
		__call = function(self,...)
			return self.Connect(...)
		end
	})

	local DATABASE = {
		__tostring = function(self)
			return self.Database..'@'..self.Hostname..':'..self.Port
		end
	}
	DATABASE.__concat = DATABASE.__tostring
	DATABASE.__index = DATABASE
	
	local STATEMENT = {
		__tostring = function(self)
			return self.Query
		end,
		__call = function(self,...)
			return self:Run(...)
		end
	}
	STATEMENT.__concat = STATEMENT.__tostring
	STATEMENT.__index = STATEMENT

	_R.MySQLDatabase = DATABASE
	_R.MySQLStatement = STATEMENT

	local tostring = tostring
	local SysTime = SysTime
	local pairs = pairs
	local select = select
	local isfunction = isfunction
	local string_format = string.format
	local string_gsub = string.gsub
	
	local color_prefix,color_text = Color(185,0,255),Color(250,250,250)

	local query_queue = {}

	function mysql.Connect(hostname,username,password,database,port,optional_socketpath,optional_clientflags,optional_connectcallback)
		local db_obj = setmetatable({
			Hostname = hostname,
			Username = username,
			Password = password,
			Database = database,
			Port = port,
		},DATABASE)

		local cached = mysql.GetTable[tostring(db_obj)]
		if cached then
			cached:Log('Recycled connection.')
			return cached
		end

		db_obj.Handle,db_obj.Error = tmysql.Connect(hostname,username,password,database,port,optional_socketpath,optional_clientflags,optional_connectcallback)

		--db_obj.Handle:Query('show tables',PrintTable)

		if db_obj.Error then
			db_obj:Log(db_obj.Error)
		elseif db_obj.Handle == false then
			db_obj:Log('Connection failed with unknown error!')
		else
			mysql.GetTable[tostring(db_obj)] = db_obj

			db_obj:Log('Connected successfully.')
		end

		hook.Add('Think',db_obj.Handle,function()
			db_obj.Handle:Poll()
		end)

		--self:SetOption(MYSQL_SET_CLIENT_IP,GetConVarString('ip'))
		--self:Connect()

		return db_obj
	end


	function DATABASE:Connect()
		return self.Handle:Connect()
	end
	
	function DATABASE:Disconnect()
		return self.Handle:Disconnect()
	end

	function DATABASE:Poll()
		self.Handle:Poll()
	end

	function DATABASE:Escape(value)
		return value ~= nil && self.Handle:Escape(tostring(value))
	end

	function DATABASE:Log(message)
		msg(color_prefix,'[MySQL] ',color_text,tostring(self)..' => '..tostring(message)..'\n')
	end

	local quote = '"'
	local retry_errors = {
		['Lost connection to MySQL server during query'] = true,
		[' MySQL server has gone away'] = true
	}
	
	local function handlequery(db,query,results,cback)
		if results[1].error ~= nil then
			db:Log(results[1].error)
			db:Log(query)
			if retry_errors[results[1].error] then
				if query_queue[query] then
					query_queue[query].Trys = query_queue[query].Trys+1
				else
					query_queue[query] = {
						Db = db,
						Query = query,
						Trys = 0,
						Cback = cback
					}
				end
			end
		elseif cback then
			cback(results[1].data,results[1].lastid,results[1].affected,results[1].time)
		end
	end
	
	function DATABASE:Query(query,...)
		local args = {...}
		local count = 0
		query = query:gsub('?',function()
			count = count+1
			return args[count] ~= nil && quote..self:Escape(args[count])..quote || 'NULL'
		end)

		self.Handle:Query(query,function(results)
			handlequery(self,query,results,args[count+1])
		end)
	end
	
	function DATABASE:QuerySync(query,...)
		local data,lastid,affected,time
		local start = SysTime()+0.3
		if ... == nil then
			self:Query(query,function(_data,_lastid,_affected,_time)
				data,lastid,affected,time = _data,_lastid,_affected,_time
			end)
		else
			self:Query(query,...,function(_data,_lastid,_affected,_time)
				data,lastid,affected,time = _data,_lastid,_affected,_time
			end)
		end

		while !data && start >= SysTime()do
			self:Poll()
		end
		return data,lastid,affected,time
	end
	
	function DATABASE:Prepare(query)
		local _,varcount = string_gsub(query,'?','?')
		local dbhandle = self.Handle
		local db = self
		local values = {}

		query = string.Replace(query,'?','%s')

		return setmetatable({
			Handle = self.Handle,
			Query = query,
			Count = varcount,
			Values = values,
			Run = function(self,...)
				local cback = select(varcount+1,...)
				for i = 1,varcount do
					local value = select(i,...)
					values[i] = value ~= nil && quote..db:Escape(value)..quote || 'NULL'
				end
				local query = string_format(query,unpack(values))
				dbhandle:Query(query,function(results)
					handlequery(db,query,results,cback)
				end)
			end,
		},STATEMENT)
	end
	
	function DATABASE:SetCharacterSet(charset)
		self.Handle:SetCharacterSet(charset)
	end

	function DATABASE:SetOption(opt,value)
		self.Handle:SetOption(opt,value)
	end

	function DATABASE:GetServerInfo()
		return self.Handle:GetServerInfo()
	end

	function DATABASE:GetHostInfo()
		return self.Handle:GetHostInfo()
	end

	function DATABASE:GetServerVersion()
		return self.Handle:GetServerVersion()
	end
	
	function STATEMENT:RunSync(...)
		local data,lastid,affected,time
		local start = SysTime()+0.3

		if ... == nil then
			self:Run(...,function(_data,_lastid,_affected,_time)
				data,lastid,affected,time = _data,_lastid,_affected,_time
			end)
		else
			self:Run(function(_data,_lastid,_affected,_time)
				data,lastid,affected,time = _data,_lastid,_affected,_time
			end)
		end

		while !data && start >= SysTime()do
			self.Handle:Poll()
		end
		return data,lastid,affected,time
	end
	
	function STATEMENT:GetQuery()
		return self.Query
	end

	function STATEMENT:GetCount()
		return self.Count
	end

	function STATEMENT:GetDatabase()
		return self.Handle
	end
	
	timer.Create('mysql.QueryQueue',0.5,0,function()
		for k,v in pairs(query_queue)do
			if v.Trys < 5 then
				v.Db:Query(v.Query,v.Cback)
				v.Trys = v.Trys+1
			else
				query_queue[k] = nil
			end
		end
	end)
elseif(file.Exists('bin/gmsv_mysqloo_linux.dll','LUA')&& system.IsLinux())||(file.Exists('bin/gmsv_mysqloo_win32.dll','LUA')&& system.IsWindows())then
	--[[
		This is taken from the mysqloolib from the mysqloo github repo. No I don't care what you say. I'm not fixing this shit, I'm not contributing to it, and you should stop using this god damn module. I'm only putting it here for stubern morons who refuse to switch to tmysql4

		Also no, I'm not doing a god damn thing to improve the mysqloolib either
		https://github.com/FredyH/MySQLOO/blob/master/lua/mysqloolib.lua
	]]
	require'mysqloo'

	msg(Color(255,0,0),'tmysql4 was not found on the server but we loaded mysqloo. Please consider switching to tmysql4 in the future!\n')
	msg(Color(255,0,0),'tmysql4 can be found here: https://github.com/SuperiorServers/gm_tmysql4\n')

	local db = {}
	local dbMetatable = {__index = db}

	--This converts an already existing database instance to be able to make use
	--of the easier functionality provided by mysqloo.CreateDatabase
	function mysqloo.ConvertDatabase(database)
		return setmetatable(database,dbMetatable)
	end

	--The same as mysqloo.connect() but adds easier functionality
	function mysqloo.CreateDatabase(...)
		local db = mysqloo.connect(...)
		db:connect()
		return mysqloo.ConvertDatabase(db)
	end

	local function addQueryFunctions(query,func,...)
		local oldtrace = debug.traceback()
		local args = {...}
		table.insert(args,query)
		function query.onAborted(qu)
			table.insert(args,false)
			table.insert(args,'aborted')
			if func then
				func(unpack(args))
			end
		end

		function query.onError(qu,err)
			table.insert(args,false)
			table.insert(args,err)
			if func then
				func(unpack(args))
			else
				ErrorNoHalt(err..'\n'..oldtrace..'\n')
			end
		end

		function query.onSuccess(qu,data)
			table.insert(args,true)
			table.insert(args,data)
			if func then
				func(unpack(args))
			end
		end
	end

	function db:RunQuery(str,callback,...)
		local query = self:query(str)
		addQueryFunctions(query,callback,...)
		query:start()
		return query
	end

	local function setPreparedQueryArguments(query,values)
		if type(values) != 'table' then
			values = {values}
		end
		local typeFunctions = {
			['string'] = function(query,index,value)query:setString(index,value)end,
			['number'] = function(query,index,value)query:setNumber(index,value)end,
			['boolean'] = function(query,index,value)query:setBoolean(index,value)end,
		}
		--This has to be pairs instead of ipairs
		--because nil is allowed as value
		for k,v in pairs(values)do
			local varType = type(v)
			if typeFunctions[varType] then
				typeFunctions[varType](query,k,v)
			else
				query:setString(k,tostring(v))
			end
		end
	end

	function db:PrepareQuery(str,values,callback,...)
		self.CachedStatements = self.CachedStatements || {}
		local preparedQuery = self.CachedStatements[str] || self:prepare(str)
		addQueryFunctions(preparedQuery,callback,...)
		setPreparedQueryArguments(preparedQuery,values)
		preparedQuery:start()
		return preparedQuery
	end

	local transaction = {}
	local transactionMT = {__index = transaction}

	function transaction:Prepare(str,values)
		--TODO: Cache queries
		local preparedQuery = self._db:prepare(str)
		setPreparedQueryArguments(preparedQuery,values)
		self:addQuery(preparedQuery)
		return preparedQuery
	end
	function transaction:Query(str)
		local query = self._db:query(str)
		self:addQuery(query)
		return query
	end

	function transaction:Start(callback,...)
		local args = {...}
		table.insert(args,self)
		function self:onSuccess()
			table.insert(args,true)
			if callback then
				callback(unpack(args))
			end
		end
		function self:onError(err)
			err = err || 'aborted'
			table.insert(args,false)
			table.insert(args,err)
			if callback then
				callback(unpack(args))
			else
				ErrorNoHalt(err)
			end
		end
		self.onAborted = self.onError
		self:start()
	end

	function db:CreateTransaction()
		local transaction = self:createTransaction()
		transaction._db = self
		setmetatable(transaction,transactionMT)
		return transaction
	end
else
	msg(Color(255,0,0),'Neither mysqloo or tmysql4 are installed! Opting for loading HTTP_SQL as fallback. This is not neccesarily an error btw, this just means that neither binary module was found on the server.\n')
	
	local http_Post = http.Post
	local http_Fetch = http.Fetch

	HTTP_SQL = {}
	
	--[[
		Expiremental atm, legit just made it because apparently people do this despite not being able to give a god damn bit of data comparing this shit to binary modules.
		You'll also have to write your own handler in either PHP or NodeJS(Use NodeJS, PHP is dogshit. only an idiot would try to argue otherwise. Fuck you if you try telling otherwise.)
		Also securing the handler is YOUR responsibility not mine. Good luck.
	

		Parameters:
			db - string - the domain to send to
			tbl - string - the mysql table you want to work with
			dat - table - the data you want to change(if the mysql database has other columns that you want to leave in tact then don't worry, just ignore them in your handler)
			whr - table - the columns and values you want to check for()
			cback - function - Callback function for if the data successfully went through
			cback_fail - function - Callback function for if the data did not successfully go through

		example:
			HTTP_SQL.Query('mydomain.com/query_handler.js','character_data',{
				name = 'Character Name',
				id = 2,
				PlayerID = '1234567890', --SteamID64() would be useful for this
			},{
				id = 2
			},function(res)
				print'Successfully sent the data'
			end,function(fail)
				print'Data did not successfully send to the db handler'
			end)
	]]
	function HTTP_SQL.Query(db,tbl,dat,whr,cback,cback_fail)
		http_Post(db,{table = tbl,data = dat,where = whr},function(res)
			if res then
				cback(res)
			end
		end,function(fail)
			if cback_fail then
				cback_fail(fail)
			end
		end)
	end
end
