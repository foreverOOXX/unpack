# -*- coding: UTF-8 -*-
import tarfile
import multiprocessing
import os
import sys
import re
import time

'''
uncompress uses the tar file absolute path for uncompress 
'''

def uncompress(abs_path,file,override=False):
    os.chdir('c:\\')
    #handle root/none-root uncompress
    temp=abs_path+str(os.getpid())
    abs_fp=file
#    print abs_fp
    if override:
        dst=override
    else:
        dst=abs_fp
    try:
        os.makedirs(temp)
        handle = tarfile.open(abs_fp,"r")
        for obj in handle:
            obj.name = re.sub(r'[:]', '_', obj.name)
            handle.extract(obj, temp)
        handle.close()
        os.remove(abs_fp)
#       print os.listdir("f:\\123")

        os.rename(temp,dst)
    except Exception as e:
        print e
        sys.exit(1)

'''
find any tar file in a given list, exclude the tar file we don't want to process.
base_dir is the local directory, something like logpath/local or clustername-1
'''
def find_tar(base_dir,ex_list=[]):
    filelist=[]
    raw=os.listdir(base_dir)
#   temp=list(set(raw) - set(ex_list))
    for i in raw:
#        print 'condition0' + ' ' + base_dir + i
        if tarfile.is_tarfile(base_dir+i):
#            print 'condition1'+' '+base_dir+i
            if i not in ex_list:
#                print 'condition2' + ' ' + base_dir+i
                filelist.append(base_dir+i)
    return filelist

def find_cluster_name(log_name):
    handle=re.match('^IsilonLogs\-(?P<cluster_name>[0-9a-zA-Z]+)\-\d+\-\d+\.tgz',log_name)
    return handle.group('cluster_name')


if __name__ == '__main__':
#    cluster_name=find_cluster_name('IsilonLogs-YZDATACENTER2-20180502-034129.tgz')
    start_time = time.time()
    alltarfile=[]
    log_path='d:\\unzip\\'
    toplevel=os.listdir(log_path)
    for toppath in toplevel:
        array=[]
        if os.path.isdir(log_path+toppath):
            inpath=log_path+toppath+'\\'

            ex_list=['node_info_cache.tar','ifsvar_modules_jobengine_cp.tar','celog_ifsvar_db.tar','minidumps.tar']

            array=find_tar(inpath,ex_list)

        if array:
            dict={'path':inpath,'tarlist':array}

            alltarfile.append(dict)


    unzip_pool = multiprocessing.Pool(processes=3)
    for i in alltarfile:
        for j in i['tarlist']:
            unzip_pool.apply_async(uncompress,args=(i['path'],j))
    unzip_pool.close()
    unzip_pool.join()

    print '%d second'% (time.time()-start_time)
#ex_list=['ifsvar_modules_jobengine_cp.tar','node_info_cache.tar']
