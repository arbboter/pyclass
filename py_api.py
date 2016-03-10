#-*- coding: utf-8 -*-
import re
import os

class PyThrift:
    thrift_dir = ''
    thrift_name = ''
    thrift_path = ''
    thrift_lib_name = ''
    service_name = ""
    name_space = []
    py_module_name = ""
    all_types = None
    all_apis = None
    api_list = {}
    types_file_name = ""
    thrift_lib_name = "mdappy"
    DeafaultParaList = ['MachineKey','Source','SceneId','Option','InReserve','Errmsg','OutReserve']
    def __init__(self,thr_path):
        self.thrift_name = os.path.basename(thr_path)
        self.thrift_dir = os.path.dirname(thr_path)
        self.thrift_path = thr_path
        
        thrift_file = open(thr_path, "r")
        
        self.service_name = self.__GetServiceName(thrift_file)
        self.name_space = self.__GetNamespace(thrift_file)
        self.py_module_name = self.thrift_name.replace(".", "_")
        
        self.all_types = self.__GetAllThriftType(thrift_file)
        self.all_apis = self.__GetAllInterface(thrift_file)
        self.__SortApi()
        
        thrift_file.close()
        
    def MyReadLine(self,fd):
        
        # 确保读取不包含注释且不为空的一行
        while True:
            line = fd.readline()
            
            if not line:
                return ""
            
            if line.find('//expired')>0:
                continue
            
            while True:                
                # // 注释
                ci = line.find('//')
                if ci > 0:
                    line = line[:ci]
                    break
            
                # #注释
                ci = line.find('#')
                if ci > 0:
                    line = line[:ci]
                    break
                
                break
            
            line = line.strip()
            if len(line)>0:
                break
            
        return line
        
    def __SortApi(self):
        for api in self.all_apis:
            idx = self.all_apis[api]["SvrId"]
            idx = int(idx[-2::],16)
            self.api_list[idx]=api
            
    def __GetServiceName(self, thr_file):
        thr_file.seek(0)
    
        SerName=None
        while True:
            line = self.MyReadLine(thr_file)
            
            if line:
                line = line.strip()
                
                if line.startswith("service"):
                    SerName=line[len("service")::].strip()
    
                    if SerName.find("/")!=-1:
                        SerName = SerName[:SerName.find("/"):].strip()
            else:
                break
        
        if SerName:
            return SerName
        else:
            print 'Get service name failed.'
            exit(0)

    def __GetNamespace(self, thr_file):
        thr_file.seek(0)
        name_space = []
        name = None
        while True:
            line = self.MyReadLine(thr_file)
            
            if line:
                line = line.strip()
                #去注释
                if line.find("/")!=-1:
                    line = line[:line.find("/"):].strip()
                
                if line.startswith("namespace"):
                    name = line[line.find("*")+1::].strip()
                    break
            else:
                break
        
        if name:
            name_space = name.split('.')
            return name_space
        else:
            print 'Get service name failed.'
            exit(0)
            
    def __GetPara(self, thr_file):
        infos = []
        while True:
            line = self.MyReadLine(thr_file)
            if not line:
                break
            
            line = line.strip()
            if line.startswith("{") or line.startswith("/"):
                continue
            
            if line.find("}")<0 and line.find(":")>0:
                info = None
                if line.rfind(",")>0:
                    info = line[line.find(':')+1:line.rfind(","):].split()
                else:
                    info = line[line.find(':')+1:line.rfind(";"):].split()
                infos.append(info)
            else:
                break
            
        return infos     
    
    def __GetAllThriftType(self, thr_file):
        thr_file.seek(0)
        
        TypeList = {}
        while True:
            line = self.MyReadLine(thr_file)
            
            if line:
                line = line.strip()
                
                if line.startswith("struct"):
                    TypeName=line[len("struct")::].strip()
                    #去注释
                    if TypeName.find("/")!=-1:
                        TypeName = TypeName[:TypeName.find("/"):].strip()
                    
                    TypeList[TypeName] = self.__GetPara(thr_file)
            else:
                break
        return TypeList
    
    def __GetApiReq(self, thr_file):
        infos = []
        while True:
            line = self.MyReadLine(thr_file)
            if not line:
                break
            
            line = line.strip()
            if line.startswith("{") or line.startswith("/"):
                continue
            
            if line.find("}")<0 and line.find(":")>0 and line.find(",")>0:
                info = line[line.find(':')+1:line.rfind(","):].split()
                infos.append(info)
            else:
                break  
        return infos  
    
    def __GetApiResp(self, thr_file):
        infos = []
        while True:
            line = self.MyReadLine(thr_file)
            if not line:
                break
            
            line = line.strip()
            if line.startswith("{") or line.startswith("/"):
                continue
            
            if line.find("}")<0 and line.find(":")>0 and line.find(",")>0:
                info = line[line.find(':')+1:line.rfind(","):].split()
                infos.append(info)
            else:
                break
            
        return infos 
    
    def __GetApi(self, thr_file):
        infos = {}
        while True:
            line = self.MyReadLine(thr_file)
            if not line:
                break
            
            line = line.strip()
            if line.startswith("{") or line.startswith("/"):
                continue
            
            LineCmd = line.upper()
            # 服务号
            if LineCmd.startswith("SVRID"):
                line = line.replace(";","")
                arr = line.split()
                infos["SvrId"] = arr[1]
                continue
            #请求
            if LineCmd.find("}")<0 and LineCmd.find("CLASS")>=0 and LineCmd.find("REQ")>=0:
                infos["req"] = self.__GetApiReq(thr_file)
                continue
            #应答
            if LineCmd.find("}")<0 and LineCmd.find("CLASS")>=0 and LineCmd.find("RESP")>=0:
                infos["resp"] = self.__GetApiResp(thr_file)
                break
            
        return infos  
        
    
    def __GetAllInterface(self, thr_file):
        thr_file.seek(0)
        
        ApiList = {}
        while True:
            line = self.MyReadLine(thr_file)
            
            if line:
                line = line.strip()
                
                LineCmd = line.upper()
                if LineCmd.startswith("APIPROTOCOL"):
                    ApiName=line[len("ApiProtocol")::].strip()
                    #去注释
                    if ApiName.find("/")!=-1:
                        ApiName = ApiName[:ApiName.find("/"):].strip()
                    
                    ApiList[ApiName] = self.__GetApi(thr_file)
                    print "%s : %s"%(ApiList[ApiName]["SvrId"], ApiName)
            else:
                break
        return ApiList
    
    
    def __InsertHead(self):
        ret_str = "#-*- coding: utf-8 -*-\n"
        ret_str+= "import time\n"
        ret_str+= "import "+self.thrift_lib_name+"\n"
        ret_str+= "from %s.rpc import make_client\n\n"%(self.thrift_lib_name)
        
        ret_str+= "class py_%s:\n\n"%(self.service_name)
        ret_str+= "    def __init__(self,idl_file,SrvHost='10.133.145.103',SrvPort=53101,Uid=0,skey=''):\n"
        ret_str+= "        self.SrvHost    = SrvHost\n"
        ret_str+= "        self.SrvPort    = SrvPort\n\n"
        
        ret_str+= "        self.LogMsg     = ''\n"
        ret_str+= "        self.ErrMsg     = ''\n"
        
        ret_str+= "        self.SceneId    = 1\n"
        ret_str+= "        self.Option     = 1\n"
        ret_str+= "        self.MachineKey = 'nicker_'+str(time.time())\n"
        ret_str+= "        self.Source     = 'py_%s'\n"%(self.service_name)
        ret_str+= "        self.InReserve  = ''\n"
        ret_str+= "        self.ModuleName = __name__\n"
        ret_str+= "        self.rCntl      = None # mdappy.rpc.CntlInfo(223039,skey='L2iN1dAVYA')\n"
        ret_str+= "        self.thrift     = mdappy.load(idl_file,module_name='%s')\n"%(self.py_module_name)
        ret_str+= "        self.srv_cli    = make_client(self.thrift.%s,SrvHost, SrvPort, cntl=self.rCntl)\n"%(self.service_name)
        return ret_str
        
    def MakeTypePrint(self,st_name):
        
        ret_str = ''
        if not self.all_types.has_key(st_name):
            return ret_str
        
        ret_str = '    def Print%s(self,oInfo):\n'%(st_name)
        st_mem = self.all_types[st_name]
        
        ret_str += "        print '-----------------------'\n"
        AllType = self.all_types
        for m in st_mem:
            mType = m[0]
            mName = m[1]
            
            if AllType.has_key(mType):
                ret_str += '        self.Print%s(oInfo.%s)\n'%(mType,mName)
            elif mType.startswith("map"):
                ret_str += "        print '%24s : '\n"%(mName)
                pat = re.compile("map.*?<(.*?),(.*?)>", re.S)
                rst = re.findall(pat, mType)
                if rst:
                    out_value = "".join(rst[0][1].split())
                    ret_str += '        if oInfo.%s : \n'%(mName)
                    ret_str += "            for v in oInfo.%s:\n"%(mName)
                    if AllType.has_key(out_value):
                        ret_str += "                self.Print%s(oInfo.%s[v])\n"%(out_value,mName)
                    else:
                        ret_str += "                print '    ',v,' = ',oInfo.%s[v]\n"%(mName)
            elif mType.startswith("list"):
                ret_str += "        print '%24s : '\n"%(mName)
                pat = re.compile("list.*?<(.*?)>", re.S)
                rst = re.findall(pat, mType)
                if rst:
                    out_value = "".join(rst[0].split())
                    ret_str += '        if oInfo.%s:\n'%(mName)
                    ret_str += "            for v in oInfo.%s:\n"%(mName)
                    if AllType.has_key(out_value):
                        ret_str += "                self.Print%s(v)\n"%(out_value)
                    else:
                        ret_str += "                print v\n"
            else:
                if mName.find('Time')>=0 and mType in ['i64']:
                    dshow = "self.CTimeToStr(oInfo.%s)"%(mName)
                    ret_str += "        print '%24s :  %%s -> %%s'%%(%s,oInfo.%s)\n"%(mName,dshow,mName)
                else:
                    ret_str += "        print '%24s : ',oInfo.%s\n"%(mName,mName)
        ret_str += "        print '-----------------------'\n"
        ret_str += '\n'
        return ret_str
    
    def __MakeDelDictNoneValue(self):
        ret_str = 'def DelDictNoneValue(InDict):\n'
        
        ret_str += '    for v in InDict.keys():\n'
        ret_str += '        if InDict[v]==None:\n'
        ret_str += '            del(InDict[v])\n'
        ret_str += '\n'
        ret_str += '    return InDict\n'
        return ret_str
        
    
    def __MakeTestApi(self, ApiName, ApiInfo):
        ApiRst = "    def Srv%s(self,oIn):\n"%(ApiName)
        ApiRst += "        oOut = []\n"
        ApiRst += "        self.AddLog('Input:%s'%(str(oIn)))\n"
        
        #入参类型检查
        para_list = ""
        AllType = {}
        for par_in in ApiInfo["req"]:
            in_type = par_in[0]
            in_name = par_in[1]
            
            #保存参数列表
            if len(para_list)<=0:
                para_list += "o"+in_name
            else:
                para_list += ",o"+in_name
                
            #自定义类型
            AllType = self.all_types
            if AllType.has_key(in_type):
                ApiRst += "        o%s = %s.%s()\n"%(in_name,"self.thrift",in_type)
                ty = AllType[in_type]
                for t in ty:
                    ApiRst += "        if oIn.has_key('%s'):\n"%(t[1])
                    ApiRst += "            o%s.%s = oIn['%s']\n"%(in_name,t[1],t[1])
            #内置类型
            else:
                ApiRst += "        if oIn.has_key('%s'):\n"%(in_name)
                ApiRst += "            o%s = oIn['%s']\n"%(in_name, in_name)
                if in_name in self.DeafaultParaList:
                    ApiRst += "        else:\n"
                    ApiRst += "            o%s = self.%s\n"%(in_name, in_name)
        ApiRst += "\n"
            
        ApiRst += "        req = %s.%s.%s_"%("self.thrift",self.service_name,ApiName)
        ApiRst += "Req(%s)\n"%(para_list)
        
        ApiRst += "        rsp = %s.%s.%s_Rsp()\n\n"%("self.thrift",self.service_name,ApiName)
        
        ApiRst += "        RetCode = 0\n"
        ApiRst += "        try:\n"
        ApiRst += "            RetCode = self.srv_cli.%s(req,rsp)\n"%(ApiName)
        ApiRst += "        except Exception as e:\n"
        ApiRst += "            self.AddError('Call %s failed, -> %%s'%%(e))\n"%(ApiName)
        ApiRst += "            return -1\n"
        
        ApiRst += "        if RetCode==0:\n"
        ApiRst += "            for o in rsp.__dict__:\n"
        ApiRst += "                if o not in ['Errmsg', 'OutReserve']:\n"
        ApiRst += "                    oOut.append(eval('rsp.'+o))\n"
        ApiRst += "        else:\n"
        ApiRst += "            self.AddError('Error[0X%X] -> %s'%(RetCode,rsp.Errmsg))\n"
        ApiRst += '        return (RetCode,oOut)\n\n'
        return ApiRst

    def __InsertTools(self):
        ret_str = '\n'
        ret_str+= "    def GetLastError(self):\n"
        ret_str+= "        return self.ErrMsg\n\n"
        
        ret_str+= "    def GetLastLog(self):\n"
        ret_str+= "        return self.LogMsg\n\n"
        
        ret_str+= "    def AddLog(self,LogStr):\n"
        ret_str+= "        self.LogMsg += '%s\\n'%(LogStr)\n\n"
        
        ret_str+= "    def AddError(self,ErrStr):\n"
        ret_str+= "        self.LogMsg += '%s\\n'%(ErrStr)\n"
        ret_str+= "        self.ErrMsg += '%s\\n'%(ErrStr)\n\n"
        
        ret_str+= "    def CTimeToStr(self,ct):\n"
        ret_str+= "        if ct:\n"
        ret_str+= "            return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(ct))\n"
        ret_str+= "        else:\n"
        ret_str+= "            return ct\n"
        
        return ret_str
    
    def __InsertMain(self):
        ret_str = '\n'
        ret_str += "if __name__ == '__main__':\n"
        ret_str += "    oStub = py_%s(r'%s','10.133.145.103')\n"%(self.service_name, self.thrift_path)
        ret_str += "    oIn = {}\n"
        ret_str += "    oOut = []\n"
        ret_str += "    RetCode = 0#oStub.xxx(oIn,oOut)\n"
        ret_str += "    if RetCode:\n"
        ret_str += "        print oStub.GetLastError()\n"
        ret_str += "    else:\n"
        ret_str += "        print 'success'\n"
        return ret_str
    
    def MakeTPrint(self):
        ret_str = "\n"
        ret_str += "    def TPrint(self,p):\n"
        ret_str += "        DefType = [int,list,dict,str,float,bool,tuple]\n"
        ret_str += "        if type(p) not in DefType:\n"
        ret_str += "            #print type(p)\n"
        ret_str += "            tp = \"self.Print\"+str(type(p)).split(\"'\")[1].strip().split('.')[-1]\n"
        ret_str += "            eval(tp)(p)\n"
        ret_str += "        elif type(p)==dict:\n"
        ret_str += "            for k in p:\n"
        ret_str += "                print k,'\\t',p[k]\n"
        ret_str += "        else:\n"
        ret_str += "            print p \n\n"
        return ret_str

    
    def MakePyClass(self,py_class_file=""):
        if not py_class_file:
            py_class_file = 'py_'+self.service_name+".py"
        out_file = open(self.thrift_dir+'\\'+py_class_file,"w")
        
        out_file.write(self.__InsertHead())
        out_file.write(self.__InsertTools())
        
        for i in self.api_list:
            api_name = self.api_list[i]
            api_info = self.all_apis[self.api_list[i]]
            out_file.write(self.__MakeTestApi(api_name, api_info))
        
        # 每个定义的Type定义输出函数
        for v in self.all_types:
            out_file.write(self.MakeTypePrint(v))
        out_file.write(self.MakeTPrint())
        
        out_file.write(self.__InsertMain())
        out_file.close()
        print "test file make finished. -> ", py_class_file
        
    def MakePyTestFile(self,py_test_file=""):
        if not py_test_file:
            py_test_file = 'py_test_'+self.service_name+".py"
        test_file = self.thrift_dir+'\\'+py_test_file
        out_file = open(test_file,"w")
        print "Test File -> ", test_file
        
        ret_str = '#-*- coding: utf-8 -*-\n'
        ret_str+= 'import py_'+self.service_name+'\n'
        ret_str += "\n"
        test_api = []
        
        # 连接
        ret_str += "pycli = py_%s.py_%s"%(self.service_name,self.service_name)
        ret_str += "(r'%s','10.133.145.103')\n\n"%(self.thrift_path)
        
        for i in self.api_list:
            api_name = self.api_list[i]
            api_info = self.all_apis[self.api_list[i]]
            
            test_api.append("%s_Test"%(api_name))
            ret_str += 'def %s_Test(pycli,oIn={}):\n'%(api_name)
            ret_str += "    if not oIn:\n"
            for para in  api_info['req']:
                name = para[1]
                ptype = para[0]
                if ptype in self.all_types:
                    for m in self.all_types[ptype]:
                        name = m[1]
                        ret_str += "        %-20s = None\n"%("oIn['%s']"%(name))
                else:
                    ret_str += "        %-20s = None\n"%("oIn['%s']"%(name))
            
            ret_str += "\n    # Delete None Value\n"
            ret_str += "    oRealIn = {}\n"
            ret_str += "    for k in oIn:\n"
            ret_str += "        if oIn[k]!=None:\n"
            ret_str += "            oRealIn[k] = oIn[k]\n"
            ret_str += "    RetCode,oOut = pycli.Srv%s(oIn)\n"%(api_name)
            ret_str += "    if RetCode:\n"
            ret_str += "        print 'Error ->', pycli.GetLastError()\n"
            ret_str += "    else:\n"
            ret_str += "        for i in oOut:\n"
            ret_str += "            pycli.TPrint(i)\n"
            ret_str += "    return RetCode\n\n"
            ret_str += "#"+api_name+"_Test(pycli)\n\n"

        out_file.write(ret_str)
        out_file.close()
    
if __name__ == '__main__':
    trunk_dir = r'F:\Share\md_trunk\server'
    idl_dir = trunk_dir + r'\idl\user'
    idl_file = idl_dir+r'\ao_user.thrift'
    test = PyThrift(idl_file)
    #test.MakePyClass()
    test.MakePyTestFile()
