#!/usr/bin/env python
# -*- coding: utf-8 -*-
# filename: tracker_client.py

import struct
import socket
from datetime import datetime
from fdfs_client.fdfs_protol import *
from fdfs_client.connection import *
from fdfs_client.exceptions import (
    FDFSError,
    ConnectionError,
    ResponseError,
    InvaildResponse,
    DataError
)
from fdfs_client.utils import *


def parse_storage_status(status_code):
    try:
        ret = {
            FDFS_STORAGE_STATUS_INIT: lambda: 'INIT',
            FDFS_STORAGE_STATUS_WAIT_SYNC: lambda: 'WAIT_SYNC',
            FDFS_STORAGE_STATUS_SYNCING: lambda: 'SYNCING',
            FDFS_STORAGE_STATUS_IP_CHANGED: lambda: 'IP_CHANGED',
            FDFS_STORAGE_STATUS_DELETED: lambda: 'DELETED',
            FDFS_STORAGE_STATUS_OFFLINE: lambda: 'OFFLINE',
            FDFS_STORAGE_STATUS_ONLINE: lambda: 'ONLINE',
            FDFS_STORAGE_STATUS_ACTIVE: lambda: 'ACTIVE',
            FDFS_STORAGE_STATUS_RECOVERY: lambda: 'RECOVERY'
        }[status_code]()
    except KeyError:
        ret = 'UNKNOW'
    return ret


class Storage_info(object):
    def __init__(self):
        self.status = 0
        self.id = ''
        self.ip_addr = ''
        self.domain_name = ''
        self.src_ip_addr = ''
        self.version = ''
        self.totalMB = ''
        self.freeMB = ''
        self.upload_prio = 0
        self.join_time = datetime.fromtimestamp(0).isoformat()
        self.up_time = datetime.fromtimestamp(0).isoformat()
        self.store_path_count = 0
        self.subdir_count_per_path = 0
        self.storage_port = 23000
        self.storage_http_port = 80
        self.curr_write_path = 0
        self.total_upload_count = 0
        self.success_upload_count = 0
        self.total_append_count = 0
        self.success_append_count = 0
        self.total_modify_count = 0
        self.success_modify_count = 0
        self.total_truncate_count = 0
        self.success_truncate_count = 0
        self.total_setmeta_count = 0
        self.success_setmeta_count = 0
        self.total_del_count = 0
        self.success_del_count = 0
        self.total_download_count = 0
        self.success_download_count = 0
        self.total_getmeta_count = 0
        self.success_getmeta_count = 0
        self.total_create_link_count = 0
        self.success_create_link_count = 0
        self.total_del_link_count = 0
        self.success_del_link_count = 0
        self.total_upload_bytes = 0
        self.success_upload_bytes = 0
        self.total_append_bytes = 0
        self.success_append_bytes = 0
        self.total_modify_bytes = 0
        self.success_modify_bytes = 0
        self.total_download_bytes = 0
        self.success_download_bytes = 0
        self.total_sync_in_bytes = 0
        self.success_sync_in_bytes = 0
        self.total_sync_out_bytes = 0
        self.success_sync_out_bytes = 0
        self.total_file_open_count = 0
        self.success_file_open_count = 0
        self.total_file_read_count = 0
        self.success_file_read_count = 0
        self.total_file_write_count = 0
        self.success_file_write_count = 0
        self.last_source_sync = datetime.fromtimestamp(0).isoformat()
        self.last_sync_update = datetime.fromtimestamp(0).isoformat()
        self.last_synced_time = datetime.fromtimestamp(0).isoformat()
        self.last_heartbeat_time = datetime.fromtimestamp(0).isoformat()
        self.if_trunk_server = 0
        # fmt = |-status(1)-ipaddr(16)-domain(128)-srcipaddr(16)-ver(6)-52*8-|
        self.fmt = '!B %ds %ds %ds %ds %ds 52QB' % (FDFS_STORAGE_ID_MAX_SIZE, \
                                                    IP_ADDRESS_SIZE, \
                                                    FDFS_DOMAIN_NAME_MAX_LEN, \
                                                    IP_ADDRESS_SIZE, \
                                                    FDFS_VERSION_SIZE)

    def set_info(self, bytes_stream):
        (self.status, id, ip_addr, domain_name, src_ip_addr, version, join_time, up_time, totalMB, freeMB, self.upload_prio,
         self.store_path_count, self.subdir_count_per_path, self.storage_port, self.storage_http_port, self.curr_write_path,
         self.total_upload_count, self.success_upload_count, self.total_append_count, self.success_append_count, self.total_modify_count, self.success_modify_count,
         self.total_truncate_count, self.success_truncate_count, self.total_setmeta_count, self.success_setmeta_count,
         self.total_del_count, self.success_del_count, self.total_download_count, self.success_download_count, self.total_getmeta_count, self.success_getmeta_count,
         self.total_create_link_count, self.success_create_link_count, self.total_del_link_count, self.success_del_link_count,
         self.total_upload_bytes, self.success_upload_bytes, self.total_append_bytes, self.total_append_bytes, self.total_modify_bytes, self.success_modify_bytes,
         self.total_download_bytes, self.success_download_bytes, self.total_sync_in_bytes, self.success_sync_in_bytes,
         self.total_sync_out_bytes, self.success_sync_out_bytes, self.total_file_open_count, self.success_file_open_count,
         self.total_file_read_count, self.success_file_read_count, self.total_file_write_count, self.success_file_write_count,
         last_source_sync, last_sync_update, last_synced_time, last_heartbeat_time, self.if_trunk_server) \
            = struct.unpack(self.fmt, bytes_stream)
        try:
            self.id = id.strip(b'\x00').decode()
            self.ip_addr = ip_addr.strip(b'\x00').decode()
            self.domain_name = domain_name.strip(b'\x00').decode()
            self.version = version.strip(b'\x00').decode()
            self.src_ip_addr = src_ip_addr.strip(b'\x00').decode()
            self.totalMB = appromix(totalMB, FDFS_SPACE_SIZE_BASE_INDEX)
            self.freeMB = appromix(freeMB, FDFS_SPACE_SIZE_BASE_INDEX)
        except ValueError as e:
            raise ResponseError('[-] Error: disk space overrun, can not represented it.')
        self.join_time = datetime.fromtimestamp(join_time).isoformat()
        self.up_time = datetime.fromtimestamp(up_time).isoformat()
        self.last_source_sync = datetime.fromtimestamp(last_source_sync).isoformat()
        self.last_sync_update = datetime.fromtimestamp(last_sync_update).isoformat()
        self.last_synced_time = datetime.fromtimestamp(last_synced_time).isoformat()
        self.last_heartbeat_time = \
            datetime.fromtimestamp(last_heartbeat_time).isoformat()
        return True

    def __str__(self):
        """Transform to readable string."""

        s = 'Storage information:\n'
        s += '\tid = %s\n' % (self.id)
        s += '\tip_addr = %s (%s)\n' % (self.ip_addr, parse_storage_status(self.status))
        s += '\thttp domain = %s\n' % self.domain_name
        s += '\tversion = %s\n' % self.version
        s += '\tjoin time = %s\n' % self.join_time
        s += '\tup time = %s\n' % self.up_time
        s += '\ttotal storage = %s\n' % self.totalMB
        s += '\tfree storage = %s\n' % self.freeMB
        s += '\tupload priority = %d\n' % self.upload_prio
        s += '\tstore path count = %d\n' % self.store_path_count
        s += '\tsubdir count per path = %d\n' % self.subdir_count_per_path
        s += '\tstorage port = %d\n' % self.storage_port
        s += '\tstorage HTTP port = %d\n' % self.storage_http_port
        s += '\tcurrent write path = %d\n' % self.curr_write_path
        s += '\tsource ip_addr = %s\n' % self.src_ip_addr
        s += '\tif_trunk_server = %d\n' % self.if_trunk_server
        s += '\ttotal upload count = %ld\n' % self.total_upload_count
        s += '\tsuccess upload count = %ld\n' % self.success_upload_count
        s += '\ttotal download count = %ld\n' % self.total_download_count
        s += '\tsuccess download count = %ld\n' % self.success_download_count
        s += '\ttotal append count = %ld\n' % self.total_append_count
        s += '\tsuccess append count = %ld\n' % self.success_append_count
        s += '\ttotal modify count = %ld\n' % self.total_modify_count
        s += '\tsuccess modify count = %ld\n' % self.success_modify_count
        s += '\ttotal truncate count = %ld\n' % self.total_truncate_count
        s += '\tsuccess truncate count = %ld\n' % self.success_truncate_count
        s += '\ttotal delete count = %ld\n' % self.total_del_count
        s += '\tsuccess delete count = %ld\n' % self.success_del_count
        s += '\ttotal set_meta count = %ld\n' % self.total_setmeta_count
        s += '\tsuccess set_meta count = %ld\n' % self.success_setmeta_count
        s += '\ttotal get_meta count = %ld\n' % self.total_getmeta_count
        s += '\tsuccess get_meta count = %ld\n' % self.success_getmeta_count
        s += '\ttotal create link count = %ld\n' % self.total_create_link_count
        s += '\tsuccess create link count = %ld\n' % self.success_create_link_count
        s += '\ttotal delete link count = %ld\n' % self.total_del_link_count
        s += '\tsuccess delete link count = %ld\n' % self.success_del_link_count
        s += '\ttotal upload bytes = %ld\n' % self.total_upload_bytes
        s += '\tsuccess upload bytes = %ld\n' % self.success_upload_bytes
        s += '\ttotal download bytes = %ld\n' % self.total_download_bytes
        s += '\tsuccess download bytes = %ld\n' % self.success_download_bytes
        s += '\ttotal append bytes = %ld\n' % self.total_append_bytes
        s += '\tsuccess append bytes = %ld\n' % self.success_append_bytes
        s += '\ttotal modify bytes = %ld\n' % self.total_modify_bytes
        s += '\tsuccess modify bytes = %ld\n' % self.success_modify_bytes
        s += '\ttotal sync_in bytes = %ld\n' % self.total_sync_in_bytes
        s += '\tsuccess sync_in bytes = %ld\n' % self.success_sync_in_bytes
        s += '\ttotal sync_out bytes = %ld\n' % self.total_sync_out_bytes
        s += '\tsuccess sync_out bytes = %ld\n' % self.success_sync_out_bytes
        s += '\ttotal file open count = %ld\n' % self.total_file_open_count
        s += '\tsuccess file open count = %ld\n' % self.success_file_open_count
        s += '\ttotal file read count = %ld\n' % self.total_file_read_count
        s += '\tsuccess file read count = %ld\n' % self.success_file_read_count
        s += '\ttotal file write count = %ld\n' % self.total_file_write_count
        s += '\tsucess file write count = %ld\n' % self.success_file_write_count
        s += '\tlast heartbeat time = %s\n' % self.last_heartbeat_time
        s += '\tlast source update = %s\n' % self.last_source_sync
        s += '\tlast sync update = %s\n' % self.last_sync_update
        s += '\tlast synced time = %s\n' % self.last_synced_time
        return s

    def get_fmt_size(self):
        return struct.calcsize(self.fmt)


class Group_info(object):
    def __init__(self):
        self.group_name = ''
        self.totalMB = ''
        self.freeMB = ''
        self.trunk_freeMB = ''
        self.count = 0
        self.storage_port = 0
        self.store_http_port = 0
        self.active_count = 0
        self.curr_write_server = 0
        self.store_path_count = 0
        self.subdir_count_per_path = 0
        self.curr_trunk_file_id = 0
        self.fmt = '!%ds 11Q' % (FDFS_GROUP_NAME_MAX_LEN + 1)
        return None

    def __str__(self):

        s = 'Group information:\n'
        s += '\tgroup name = %s\n' % self.group_name
        s += '\tdisk total space = %s\n' % self.totalMB
        s += '\tdisk free space = %s\n' % self.freeMB
        s += '\ttrunk free space = %s\n' % self.trunk_freeMB
        s += '\tstorage server count = %d\n' % self.count
        s += '\tstorage port = %d\n' % self.storage_port
        s += '\tstorage HTTP port = %d\n' % self.store_http_port
        s += '\tactive server count = %d\n' % self.active_count
        s += '\tcurrent write server index = %d\n' % self.curr_write_server
        s += '\tstore path count = %d\n' % self.store_path_count
        s += '\tsubdir count per path = %d\n' % self.subdir_count_per_path
        s += '\tcurrent trunk file id = %d\n' % self.curr_trunk_file_id
        return s

    def set_info(self, bytes_stream):
        (group_name, totalMB, freeMB, trunk_freeMB, self.count, self.storage_port, \
         self.store_http_port, self.active_count, self.curr_write_server, \
         self.store_path_count, self.subdir_count_per_path, self.curr_trunk_file_id) \
            = struct.unpack(self.fmt, bytes_stream)
        try:
            self.group_name = group_name.strip(b'\x00').decode()
            self.totalMB = appromix(totalMB, FDFS_SPACE_SIZE_BASE_INDEX)
            self.freeMB = appromix(freeMB, FDFS_SPACE_SIZE_BASE_INDEX)
            self.trunk_freeMB = appromix(trunk_freeMB, FDFS_SPACE_SIZE_BASE_INDEX)
        except ValueError:
            raise DataError('[-] Error disk space overrun, can not represented it.')

    def get_fmt_size(self):
        return struct.calcsize(self.fmt)


class Tracker_client(object):
    """Class Tracker client."""

    def __init__(self, pool):
        self.pool = pool

    def tracker_list_servers(self, group_name, storage_ip=None):
        """
        List servers in a storage group
        """
        conn = self.pool.get_connection()
        th = Tracker_header()
        ip_len = len(storage_ip) if storage_ip else 0
        if ip_len >= IP_ADDRESS_SIZE:
            ip_len = IP_ADDRESS_SIZE - 1
        th.pkg_len = FDFS_GROUP_NAME_MAX_LEN + ip_len
        th.cmd = TRACKER_PROTO_CMD_SERVER_LIST_STORAGE
        group_fmt = '!%ds' % FDFS_GROUP_NAME_MAX_LEN
        store_ip_addr = storage_ip or ''
        storage_ip_fmt = '!%ds' % ip_len
        try:
            th.send_header(conn)
            send_buffer = struct.pack(group_fmt, group_name) + \
                          struct.pack(storage_ip_fmt, store_ip_addr)
            tcp_send_data(conn, send_buffer)
            th.recv_header(conn)
            if th.status != 0:
                raise DataError('[-] Error: %d, %s' % (th.status, os.strerror(th.status)))
            recv_buffer, recv_size = tcp_recv_response(conn, th.pkg_len)
            si = Storage_info()
            si_fmt_size = si.get_fmt_size()
            recv_size = len(recv_buffer)
            if recv_size % si_fmt_size != 0:
                errinfo = '[-] Error: response size not match, expect: %d, actual: %d' \
                          % (th.pkg_len, recv_size)
                raise ResponseError(errinfo)
        except ConnectionError:
            conn.disconnect()
            raise
        finally:
            self.pool.release(conn)
        num_storage = recv_size / si_fmt_size
        si_list = []
        i = 0
        while num_storage:
            si.set_info(recv_buffer[(i * si_fmt_size): ((i + 1) * si_fmt_size)])
            si_list.append(si)
            si = Storage_info()
            num_storage -= 1
            i += 1
        ret_dict = {}
        ret_dict['Group name'] = group_name
        ret_dict['Servers'] = si_list
        return ret_dict

    def tracker_list_one_group(self, group_name):
        conn = self.pool.get_connection()
        th = Tracker_header()
        th.pkg_len = FDFS_GROUP_NAME_MAX_LEN
        th.cmd = TRACKER_PROTO_CMD_SERVER_LIST_ONE_GROUP
        # group_fmt: |-group_name(16)-|
        group_fmt = '!%ds' % FDFS_GROUP_NAME_MAX_LEN
        try:
            th.send_header(conn)
            send_buffer = struct.pack(group_fmt, group_name)
            tcp_send_data(conn, send_buffer)
            th.recv_header(conn)
            if th.status != 0:
                raise DataError('[-] Error: %d, %s' % (th.status, os.strerror(th.status)))
            recv_buffer, recv_size = tcp_recv_response(conn, th.pkg_len)
            group_info = Group_info()
            group_info.set_info(recv_buffer)
        except ConnectionError:
            conn.disconnect()
            raise
        finally:
            self.pool.release(conn)
        return group_info

    def tracker_list_all_groups(self):
        conn = self.pool.get_connection()
        th = Tracker_header()
        th.cmd = TRACKER_PROTO_CMD_SERVER_LIST_ALL_GROUPS
        try:
            th.send_header(conn)
            th.recv_header(conn)
            if th.status != 0:
                raise DataError('[-] Error: %d, %s' % (th.status, os.strerror(th.status)))
            recv_buffer, recv_size = tcp_recv_response(conn, th.pkg_len)
        except ConnectionError:
            conn.disconnect()
            raise
        finally:
            self.pool.release(conn)
        gi = Group_info()
        gi_fmt_size = gi.get_fmt_size()
        if recv_size % gi_fmt_size != 0:
            errmsg = '[-] Error: Response size is mismatch, except: %d, actul: %d' \
                     % (th.pkg_len, recv_size)
            raise ResponseError(errmsg)
        num_groups = recv_size / gi_fmt_size
        ret_dict = {}
        ret_dict['Groups count'] = num_groups
        gi_list = []
        i = 0
        while num_groups:
            gi.set_info(recv_buffer[i * gi_fmt_size: (i + 1) * gi_fmt_size])
            gi_list.append(gi)
            gi = Group_info()
            i += 1
            num_groups -= 1
        ret_dict['Groups'] = gi_list
        return ret_dict

    def tracker_query_storage_stor_without_group(self):
        """Query storage server for upload, without group name.
        Return: Storage_server object"""
        conn = self.pool.get_connection()
        th = Tracker_header()
        th.cmd = TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE
        try:
            th.send_header(conn)
            th.recv_header(conn)
            if th.status != 0:
                raise DataError('[-] Error: %d, %s' % (th.status, os.strerror(th.status)))
            recv_buffer, recv_size = tcp_recv_response(conn, th.pkg_len)
            if recv_size != TRACKER_QUERY_STORAGE_STORE_BODY_LEN:
                errmsg = '[-] Error: Tracker response length is invaild, '
                errmsg += 'expect: %d, actual: %d' \
                          % (TRACKER_QUERY_STORAGE_STORE_BODY_LEN, recv_size)
                raise ResponseError(errmsg)
        except ConnectionError:
            conn.disconnect()
            raise
        finally:
            self.pool.release(conn)
        # recv_fmt |-group_name(16)-ipaddr(16-1)-port(8)-store_path_index(1)|
        recv_fmt = '!%ds %ds Q B' % (FDFS_GROUP_NAME_MAX_LEN, IP_ADDRESS_SIZE - 1)
        store_serv = Storage_server()
        (group_name, ip_addr, \
         store_serv.port, store_serv.store_path_index) = struct.unpack(recv_fmt, recv_buffer)
        store_serv.group_name = group_name.strip(b'\x00').decode()
        store_serv.ip_addr = ip_addr.strip(b'\x00').decode()
        return store_serv

    def tracker_query_storage_stor_with_group(self, group_name):
        """Query storage server for upload, based group name.
        arguments:
        @group_name: string
        @Return Storage_server object
        """
        conn = self.pool.get_connection()
        th = Tracker_header()
        th.cmd = TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE
        th.pkg_len = FDFS_GROUP_NAME_MAX_LEN
        th.send_header(conn)
        group_fmt = '!%ds' % FDFS_GROUP_NAME_MAX_LEN
        send_buffer = struct.pack(group_fmt, group_name)
        try:
            tcp_send_data(conn, send_buffer)
            th.recv_header(conn)
            if th.status != 0:
                raise DataError('Error: %d, %s' % (th.status, os.strerror(th.status)))
            recv_buffer, recv_size = tcp_recv_response(conn, th.pkg_len)
            if recv_size != TRACKER_QUERY_STORAGE_STORE_BODY_LEN:
                errmsg = '[-] Error: Tracker response length is invaild, '
                errmsg += 'expect: %d, actual: %d' \
                          % (TRACKER_QUERY_STORAGE_STORE_BODY_LEN, recv_size)
                raise ResponseError(errmsg)
        except ConnectionError:
            conn.disconnect()
            raise
        finally:
            self.pool.release(conn)
        # recv_fmt: |-group_name(16)-ipaddr(16-1)-port(8)-store_path_index(1)-|
        recv_fmt = '!%ds %ds Q B' % (FDFS_GROUP_NAME_MAX_LEN, IP_ADDRESS_SIZE - 1)
        store_serv = Storage_server()
        (group, ip_addr, \
         store_serv.port, store_serv.store_path_index) = struct.unpack(recv_fmt, recv_buffer)
        store_serv.group_name = group.strip(b'\x00').decode()
        store_serv.ip_addr = ip_addr.strip(b'\x00').decode()
        return store_serv

    def _tracker_do_query_storage(self, group_name, filename, cmd):
        """
        core of query storage, based group name and filename. 
        It is useful download, delete and set meta.
        arguments:
        @group_name: string
        @filename: string. remote file_id
        @Return: Storage_server object
        """
        conn = self.pool.get_connection()
        th = Tracker_header()
        file_name_len = len(filename)
        th.pkg_len = FDFS_GROUP_NAME_MAX_LEN + file_name_len
        th.cmd = cmd
        th.send_header(conn)
        # query_fmt: |-group_name(16)-filename(file_name_len)-|
        query_fmt = '!%ds %ds' % (FDFS_GROUP_NAME_MAX_LEN, file_name_len)
        send_buffer = struct.pack(query_fmt, group_name.encode(), filename.encode())
        try:
            tcp_send_data(conn, send_buffer)
            th.recv_header(conn)
            if th.status != 0:
                raise DataError('Error: %d, %s' % (th.status, os.strerror(th.status)))
            recv_buffer, recv_size = tcp_recv_response(conn, th.pkg_len)
            if recv_size != TRACKER_QUERY_STORAGE_FETCH_BODY_LEN:
                errmsg = '[-] Error: Tracker response length is invaild, '
                errmsg += 'expect: %d, actual: %d' % (th.pkg_len, recv_size)
                raise ResponseError(errmsg)
        except ConnectionError:
            conn.disconnect()
            raise
        finally:
            self.pool.release(conn)
        # recv_fmt: |-group_name(16)-ip_addr(16)-port(8)-|
        recv_fmt = '!%ds %ds Q' % (FDFS_GROUP_NAME_MAX_LEN, IP_ADDRESS_SIZE - 1)
        store_serv = Storage_server()
        (group_name, ipaddr, store_serv.port) = struct.unpack(recv_fmt, recv_buffer)
        store_serv.group_name = group_name.strip(b'\x00').decode()
        store_serv.ip_addr = ipaddr.strip(b'\x00').decode()
        return store_serv

    def tracker_query_storage_update(self, group_name, filename):
        """
        Query storage server to update(delete and set_meta).
        """
        return self._tracker_do_query_storage(group_name, filename, TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE)

    def tracker_query_storage_fetch(self, group_name, filename):
        """
        Query storage server to download.
        """
        return self._tracker_do_query_storage(group_name, filename, TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE)
