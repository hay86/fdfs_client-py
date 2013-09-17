#!/usr/bin/env python
# -*- coding: utf-8 -*-
# filename: exceptions.py

'''Core exceptions raised by fdfs client'''

class FDFSError(Exception):
    pass

class ConnectionError(FDFSError):
    pass

class ResponseError(FDFSError):
    pass

class InvaildResponse(FDFSError):
    pass

class DataError(FDFSError):
    pass

