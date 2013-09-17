#!/usr/bin/env python
# -*- coding: utf-8 -*-
# filename: client.py

'''
  Client module for Fastdfs 3.08
  author: scott yuan scottzer8@gmail.com
  date: 2012-06-21
'''

import os, sys
from fdfs_client.utils import *
from fdfs_client.tracker_client import *
from fdfs_client.storage_client import *
from fdfs_client.exceptions import *

def get_tracker_conf(conf_path = 'client.conf'):
    cf = Fdfs_ConfigParser()
    tracker = {}
    try:
        cf.read(conf_path)
        timeout = cf.getint('__config__', 'connect_timeout')
        tracker_list = cf.get('__config__', 'tracker_server')
        if isinstance(tracker_list, str):
            tracker_list = [tracker_list]
        tracker_ip_list = []
        for tr in tracker_list:
            tracker_ip, tracker_port = tr.split(':')
            tracker_ip_list.append(tracker_ip)
        tracker['host_tuple'] = tuple(tracker_ip_list)
        tracker['port']       = int(tracker_port)
        tracker['timeout']    = timeout
        tracker['name']       = 'Tracker Pool'
    except:
        raise
    return tracker

class Fdfs_client(object):
    '''
    Class Fdfs_client implemented Fastdfs client protol ver 3.08.

    It's useful upload, download, delete file to or from fdfs server, etc. It's uses
    connection pool to manage connection to server.
    '''
    def __init__(self, conf_path = '/etc/fdfs/client.conf', \
                 poolclass =ConnectionPool):
        self.trackers = get_tracker_conf(conf_path)
        self.tracker_pool = poolclass(**self.trackers)
        self.timeout  = self.trackers['timeout']
        return None

    def __del__(self):
        try:
            self.pool.destroy()
            self.pool = None
        except:
            pass

    def upload_by_filename(self, filename, meta_dict = None):
        '''
        Upload a file to Storage server.
        arguments:
        @filename: string, name of file that will be uploaded
        @meta_dict: dictionary e.g.:{
            'ext_name'  : 'jpg',
            'file_size' : '10240B',
            'width'     : '160px',
            'hight'     : '80px'
        } meta_dict can be null
        @return dict {
            'Group name'      : group_name,
            'Remote file_id'  : remote_file_id,
            'Status'          : 'Upload successed.',
            'Local file name' : local_file_name,
            'Uploaded size'   : upload_size,
            'Storage IP'      : storage_ip
        } if success else None
        '''
        isfile, errmsg = fdfs_check_file(filename)
        if not isfile:
            raise DataError(errmsg + '(uploading)')
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_stor_without_group()
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_upload_by_filename(tc, store_serv, filename, meta_dict)

    def upload_by_file(self, filename, meta_dict = None):
        isfile, errmsg = fdfs_check_file(filename)
        if not isfile:
            raise DataError(errmsg + '(uploading)')
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_stor_without_group()
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_upload_by_file(tc, store_serv, filename, meta_dict)

    def upload_by_buffer(self, filebuffer, file_ext_name = None, meta_dict = None):
        '''
        Upload a buffer to Storage server.
        arguments:
        @filebuffer: string, buffer
        @file_ext_name: string, file extend name
        @meta_dict: dictionary e.g.:{
            'ext_name'  : 'jpg',
            'file_size' : '10240B',
            'width'     : '160px',
            'hight'     : '80px'
        }
        @return dict {
            'Group name'      : group_name,
            'Remote file_id'  : remote_file_id,
            'Status'          : 'Upload successed.',
            'Local file name' : '',
            'Uploaded size'   : upload_size,
            'Storage IP'      : storage_ip
        } if success else None
        '''
        if not filebuffer:
            raise DataError('[-] Error: argument filebuffer can not be null.')
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_stor_without_group()
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_upload_by_buffer(tc, store_serv, filebuffer, \
                                              file_ext_name, meta_dict)

    def upload_slave_by_filename(self, filename, remote_file_id, prefix_name, \
                                 meta_dict = None):
        '''
        Upload slave file to Storage server.
        arguments:
        @filename: string, local file name
        @remote_file_id: string, remote file id
        @prefix_name: string
        @meta_dict: dictionary e.g.:{
            'ext_name'  : 'jpg',
            'file_size' : '10240B',
            'width'     : '160px',
            'hight'     : '80px'
        }
        @return dictionary {
            'Status'        : 'Upload slave successed.',
            'Local file name' : local_filename,
            'Uploaded size'   : upload_size,
            'Remote file id'  : remote_file_id,
            'Storage IP'      : storage_ip
        }
        '''
        isfile, errmsg = fdfs_check_file(filename)
        if not isfile:
            raise DataError(errmsg + '(uploading slave)')
        tmp = split_remote_fileid(remote_file_id)
        if not tmp:
            raise DataError('[-] Error: remote_file_id is invalid.(uploading slave)')
        if not prefix_name:
            raise DataError('[-] Error: prefix_name can not be null.')
        group_name, remote_filename = tmp
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_stor_with_group(group_name)
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        try:
            ret_dict = store.storage_upload_slave_by_filename(tc, store_serv, filename, \
                                                          prefix_name, remote_filename, \
                                                          meta_dict = None)
        except:
            raise
        ret_dict['Status'] = 'Upload slave file successed.'
        return ret_dict

    def upload_slave_by_file(self, filename, remote_file_id, prefix_name, \
                                 meta_dict = None):
        '''
        Upload slave file to Storage server.
        arguments:
        @filename: string, local file name
        @remote_file_id: string, remote file id
        @prefix_name: string
        @meta_dict: dictionary e.g.:{
            'ext_name'  : 'jpg',
            'file_size' : '10240B',
            'width'     : '160px',
            'hight'     : '80px'
        }
        @return dictionary {
            'Status'        : 'Upload slave successed.',
            'Local file name' : local_filename,
            'Uploaded size'   : upload_size,
            'Remote file id'  : remote_file_id,
            'Storage IP'      : storage_ip
        }
        '''
        isfile, errmsg = fdfs_check_file(filename)
        if not isfile:
            raise DataError(errmsg + '(uploading slave)')
        tmp = split_remote_fileid(remote_file_id)
        if not tmp:
            raise DataError('[-] Error: remote_file_id is invalid.(uploading slave)')
        if not prefix_name:
            raise DataError('[-] Error: prefix_name can not be null.')
        group_name, remote_filename = tmp
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_stor_with_group(group_name)
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        try:
            ret_dict = store.storage_upload_slave_by_file(tc, store_serv, filename, \
                                                          prefix_name, remote_filename, \
                                                          meta_dict = None)
        except:
            raise
        ret_dict['Status'] = 'Upload slave file successed.'
        return ret_dict

    def upload_slave_by_buffer(self, filebuffer, remote_file_id, \
                               meta_dict = None, file_ext_name = None):
        '''
        Upload slave file by buffer
        arguments:
        @filebuffer: string
        @remote_file_id: string
        @meta_dict: dictionary e.g.:{
            'ext_name'  : 'jpg',
            'file_size' : '10240B',
            'width'     : '160px',
            'hight'     : '80px'
        }
        @return dictionary {
            'Status'        : 'Upload slave successed.',
            'Local file name' : local_filename,
            'Uploaded size'   : upload_size,
            'Remote file id'  : remote_file_id,
            'Storage IP'      : storage_ip
        }
        '''
        if not filebuffer:
            raise DataError('[-] Error: argument filebuffer can not be null.')
        tmp = split_remote_fileid(remote_file_id)
        if not tmp:
            raise DataError('[-] Error: remote_file_id is invalid.(uploading slave)')
        group_name, remote_filename = tmp
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_update(group_name, remote_filename)
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_upload_slave_by_buffer(tc, store_serv, filebuffer, \
                                                    remote_filename, meta_dict, \
                                                    file_ext_name)
            
    def upload_appender_by_filename(self, local_filename, meta_dict = None):
        '''
        Upload an appender file by filename.
        arguments:
        @local_filename: string
        @meta_dict: dictionary e.g.:{
            'ext_name'  : 'jpg',
            'file_size' : '10240B',
            'width'     : '160px',
            'hight'     : '80px'
        }    Notice: it can be null
        @return dict {
            'Group name'      : group_name,
            'Remote file_id'  : remote_file_id,
            'Status'          : 'Upload successed.',
            'Local file name' : '',
            'Uploaded size'   : upload_size,
            'Storage IP'      : storage_ip
        } if success else None
        '''
        isfile, errmsg = fdfs_check_file(local_filename)
        if not isfile:
            raise DataError(errmsg + '(uploading appender)')
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_stor_without_group()
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_upload_appender_by_filename(tc, store_serv, \
                                                         local_filename, meta_dict)

    def upload_appender_by_file(self, local_filename, meta_dict = None):
        '''
        Upload an appender file by file.
        arguments:
        @local_filename: string
        @meta_dict: dictionary e.g.:{
            'ext_name'  : 'jpg',
            'file_size' : '10240B',
            'width'     : '160px',
            'hight'     : '80px'
        }    Notice: it can be null
        @return dict {
            'Group name'      : group_name,
            'Remote file_id'  : remote_file_id,
            'Status'          : 'Upload successed.',
            'Local file name' : '',
            'Uploaded size'   : upload_size,
            'Storage IP'      : storage_ip
        } if success else None
        '''
        isfile, errmsg = fdfs_check_file(local_filename)
        if not isfile:
            raise DataError(errmsg + '(uploading appender)')
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_stor_without_group()
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_upload_appender_by_file(tc, store_serv, \
                                                         local_filename, meta_dict)

    def upload_appender_by_buffer(self, filebuffer, file_ext_name = None, meta_dict = None):
        '''
        Upload a buffer to Storage server.
        arguments:
        @filebuffer: string
        @file_ext_name: string, can be null
        @meta_dict: dictionary, can be null
        @return dict {
            'Group name'      : group_name,
            'Remote file_id'  : remote_file_id,
            'Status'          : 'Upload successed.',
            'Local file name' : '',
            'Uploaded size'   : upload_size,
            'Storage IP'      : storage_ip
        } if success else None
        '''
        if not filebuffer:
            raise DataError('[-] Error: argument filebuffer can not be null.')
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_stor_without_group()
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_upload_appender_by_buffer(tc, store_serv, \
                                                       filebuffer, meta_dict, \
                                                       file_ext_name)

    def delete_file(self, remote_file_id):
        '''
        Delete a file from Storage server.
        arguments:
        @remote_file_id: string, file_id of file that is on storage server
        @return tuple ('Delete file successed.', remote_file_id, storage_ip)
        '''
        tmp = split_remote_fileid(remote_file_id)
        if not tmp:
            raise DataError('[-] Error: remote_file_id is invalid.(in delete file)')
        group_name, remote_filename = tmp
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_stor_with_group(group_name)
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_delete_file(tc, store_serv, remote_filename)

    def download_to_file(self, local_filename, remote_file_id, offset = 0, down_bytes = 0):
        '''
        Download a file from Storage server.
        arguments:
        @local_filename: string, local name of file 
        @remote_file_id: string, file_id of file that is on storage server
        @offset: long
        @downbytes: long
        @return dict {
            'Remote file_id'  : remote_file_id,
            'Content'         : local_filename,
            'Download size'   : downloaded_size,
            'Storage IP'      : storage_ip
        }
        '''
        tmp = split_remote_fileid(remote_file_id)
        if not tmp:
            raise DataError('[-] Error: remote_file_id is invalid.(in download file)')
        group_name, remote_filename = tmp
        if not offset:
            file_offset = long(offset)
        if not down_bytes:
            download_bytes = long(down_bytes)
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_stor_with_group(group_name)
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_download_to_file(tc, store_serv, local_filename, \
                                              file_offset, download_bytes, \
                                              remote_filename)

    def download_to_buffer(self, remote_file_id, offset = 0, down_bytes = 0):
        '''
        Download a file from Storage server and store in buffer.
        arguments:
        @remote_file_id: string, file_id of file that is on storage server
        @offset: long
        @down_bytes: long
        @return dict {
            'Remote file_id'  : remote_file_id,
            'Content'         : file_buffer,
            'Download size'   : downloaded_size,
            'Storage IP'      : storage_ip
        }
        '''
        tmp = split_remote_fileid(remote_file_id)
        if not tmp:
            raise DataError('[-] Error: remote_file_id is invalid.(in download file)')
        group_name, remote_filename = tmp
        if not offset:
            file_offset = long(offset)
        if not down_bytes:
            download_bytes = long(down_bytes)
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_stor_with_group(group_name)
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        file_buffer = None
        return store.storage_download_to_buffer(tc, store_serv, file_buffer, \
                                                file_offset, download_bytes, \
                                              remote_filename)
                                              
    def list_one_group(self, group_name):
        '''
        List one group information.
        arguments:
        @group_name: string, group name will be list
        @return Group_info,  instance
        '''
        tc = Tracker_client(self.tracker_pool)
        return tc.tracker_list_one_group(group_name)

    def list_servers(self, group_name, storage_ip = None):
        '''
        List all storage servers information in a group
        arguments:
        @group_name: string
        @return dictionary {
            'Group name' : group_name,
            'Servers'    : server list,
        }
        '''
        tc = Tracker_client(self.tracker_pool)
        return tc.tracker_list_servers(group_name, storage_ip)

    def list_all_groups(self):
        '''
        List all group information.
        @return dictionary {
            'Groups count' : group_count,
            'Groups'       : list of groups
        }
        '''
        tc = Tracker_client(self.tracker_pool)
        return tc.tracker_list_all_groups()

    def get_meta_data(self, remote_file_id):
        '''
        Get meta data of remote file.
        arguments:
        @remote_fileid: string, remote file id
        @return dictionary, meta data
        '''
        tmp = split_remote_fileid(remote_file_id)
        if not tmp:
            raise DataError('[-] Error: remote_file_id is invalid.(in get meta data)')
        group_name, remote_filename = tmp
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_update(group_name, remote_filename)
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_get_metadata(tc, store_serv, remote_filename)

    def set_meta_data(self, remote_file_id, \
                      meta_dict, op_flag = STORAGE_SET_METADATA_FLAG_OVERWRITE):
        '''
        Set meta data of remote file.
        arguments:
        @remote_file_id: string
        @meta_dict: dictionary
        @op_flag: char, 'O' for overwrite, 'M' for merge
        @return dictionary {
            'Status'     : status,
            'Storage IP' : storage_ip
        }
        '''
        tmp = split_remote_fileid(remote_file_id)
        if not tmp:
            raise DataError('[-] Error: remote_file_id is invalid.(in set meta data)')
        group_name, remote_filename = tmp
        tc = Tracker_client(self.tracker_pool)
        try:
            store_serv = tc.tracker_query_storage_update(group_name, remote_filename)
            store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
            status = store.storage_set_metadata(tc, store_serv, \
                                                remote_filename, meta_dict)
        except (ConnectionError, ResponseError, DataError):
            raise
        #if status == 2:
        #    raise DataError('[-] Error: remote file %s is not exist.' % remote_file_id)
        if status != 0:
            raise DataError('[-] Error: %d, %s' % (th.status, os.strerror(th.status)))
        ret_dict = {}
        ret_dict['Status'] = 'Set meta data success.'
        ret_dict['Storage IP'] = store_serv.ip_addr
        return ret_dict

    def append_by_filename(self, local_filename, remote_fileid):
        isfile, errmsg = fdfs_check_file(local_filename)
        if not isfile:
            raise DataError(errmsg + '(append)')
        tmp = split_remote_fileid(remote_fileid)
        if not tmp:
            raise DataError('[-] Error: remote_file_id is invalid.(append)')
        group_name, appended_filename = tmp
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_update(group_name, appended_filename)
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_append_by_filename(tc, store_serv, local_filename, \
                                                appended_filename)

    def append_by_file(self, local_filename, remote_fileid):
        isfile, errmsg = fdfs_check_file(local_filename)
        if not isfile:
            raise DataError(errmsg + '(append)')
        tmp = split_remote_fileid(remote_fileid)
        if not tmp:
            raise DataError('[-] Error: remote_file_id is invalid.(append)')
        group_name, appended_filename = tmp
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_update(group_name, appended_filename)
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_append_by_file(tc, store_serv, local_filename, \
                                                appended_filename)

    def append_by_buffer(self, file_buffer, remote_fileid):
        if not file_buffer:
            raise DataError('[-] Error: file_buffer can not be null.')
        tmp = split_remote_fileid(remote_fileid)
        if not tmp:
            raise DataError('[-] Error: remote_file_id is invalid.(append)')
        group_name, appended_filename = tmp
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_update(group_name, appended_filename)
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_append_by_buffer(tc, store_serv, file_buffer, \
                                                appended_filename)


    def truncate_file(self, truncated_filesize, appender_fileid):
        '''
        Truncate file in Storage server.
        arguments:
        @truncated_filesize: long
        @appender_fileid: remote_fileid
        @return: dictionary {
            'Status'     : 'Truncate successed.',
            'Storage IP' : storage_ip
        }
        '''
        trunc_filesize = long(truncated_filesize)
        tmp = split_remote_fileid(appender_fileid)
        if not tmp:
            raise DataError('[-] Error: appender_fileid is invalid.(truncate)')
        group_name, appender_filename = tmp
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_update(group_name, appender_filename)
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_truncate_file(tc, store_serv, trunc_filesize, \
                                           appender_filename)
        
    def modify_by_filename(self, filename, appender_fileid, offset = 0):
        '''
        Modify a file in Storage server by file.
        arguments:
        @filename: string, local file name
        @offset: long, file offset
        @appender_fileid: string, remote file id
        @return: dictionary {
            'Status'     : 'Modify successed.',
            'Storage IP' : storage_ip
        }
        '''
        isfile, errmsg = fdfs_check_file(filename)
        if not isfile:
            raise DataError(errmsg + '(modify)')
        filesize = os.stat(filename).st_size
        tmp = split_remote_fileid(appender_fileid)
        if not tmp:
            raise DataError('[-] Error: remote_fileid is invalid.(modify)')
        group_name, appender_filename = tmp
        if not offset:
            file_offset = long(offset)
        else:
            file_offset = 0
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_update(group_name, appender_filename)
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_modify_by_filename(tc, store_serv, filename, file_offset, \
                                                filesize, appender_filename)

    def modify_by_file(self, filename, appender_fileid, offset = 0):
        '''
        Modify a file in Storage server by file.
        arguments:
        @filename: string, local file name
        @offset: long, file offset
        @appender_fileid: string, remote file id
        @return: dictionary {
            'Status'     : 'Modify successed.',
            'Storage IP' : storage_ip
        }
        '''
        isfile, errmsg = fdfs_check_file(filename)
        if not isfile:
            raise DataError(errmsg + '(modify)')
        filesize = os.stat(filename).st_size
        tmp = split_remote_fileid(appender_fileid)
        if not tmp:
            raise DataError('[-] Error: remote_fileid is invalid.(modify)')
        group_name, appender_filename = tmp
        if not offset:
            file_offset = long(offset)
        else:
            file_offset = 0
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_update(group_name, appender_filename)
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_modify_by_file(tc, store_serv, filename, file_offset, \
                                                filesize, appender_filename)

    def modify_by_buffer(self, filebuffer, appender_fileid, offset = 0):
        '''
        Modify a file in Storage server by buffer.
        arguments:
        @filebuffer: string, file buffer
        @offset: long, file offset
        @appender_fileid: string, remote file id
        @return: dictionary {
            'Status'     : 'Modify successed.',
            'Storage IP' : storage_ip
        }
        '''
        if not filebuffer:
            raise DataError('[-] Error: filebuffer can not be null.(modify)')
        filesize = len(filebuffer)
        tmp = split_remote_fileid(appender_fileid)
        if not tmp:
            raise DataError('[-] Error: remote_fileid is invalid.(modify)')
        group_name, appender_filename = tmp
        if not offset:
            file_offset = long(offset)
        else:
            file_offset = 0
        tc = Tracker_client(self.tracker_pool)
        store_serv = tc.tracker_query_storage_update(group_name, appender_filename)
        store = Storage_client(store_serv.ip_addr, store_serv.port, self.timeout)
        return store.storage_modify_by_buffer(tc, store_serv, filebuffer, file_offset, \
                                                filesize, appender_filename)
        
    
