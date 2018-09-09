#!/usr/bin/env python
# -*- coding = utf-8 -*-
# filename: utils.py
from configparser import DEFAULTSECT, MissingSectionHeaderError, ParsingError, RawConfigParser, \
    NoSectionError
import os
import stat
from mutagen._compat import StringIO

from requests.compat import basestring

SUFFIX = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']


def appromix(size, base=0):
    """Conver bytes stream size to human-readable format.
    Keyword arguments:
    size: int, bytes stream size
    base: int, suffix index
    Return: string
    """
    multiples = 1024
    if size < 0:
        raise ValueError('[-] Error: number must be non-negative.')
    if size < multiples:
        return '{0:d}{1}'.format(size, SUFFIX[base])
    for suffix in SUFFIX[base:]:
        if size < multiples:
            return '{0:.2f}{1}'.format(size, suffix)
        size //= multiples
    raise ValueError('[-] Error: number too big.')


def get_file_ext_name(filename, double_ext=True):
    li = filename.split(os.extsep)
    if len(li) <= 1:
        return ''
    else:
        if li[-1].find(os.sep) != -1:
            return ''
    if double_ext:
        if len(li) > 2:
            if li[-2].find(os.sep) == -1:
                return '%s.%s' % (li[-2], li[-1])
    return li[-1]


class Fdfs_ConfigParser(RawConfigParser):
    """ 
    Extends ConfigParser to allow files without sections. 
 
    This is done by wrapping read files and prepending them with a placeholder 
    section, which defaults to '__config__' 
    """

    def __init__(self, default_section=None, *args, **kwargs):
        RawConfigParser.__init__(self, *args, **kwargs)

        self._default_section = None
        self.set_default_section(default_section or '__config__')

    def get_default_section(self):
        return self._default_section

    def set_default_section(self, section):
        self.add_section(section)

        # move all values from the previous default section to the new one 
        try:
            default_section_items = self.items(self._default_section)
            self.remove_section(self._default_section)
        except NoSectionError:
            pass
        else:
            for (key, value) in default_section_items:
                self.set(section, key, value)

        self._default_section = section

    def read(self, filenames):
        if isinstance(filenames, basestring):
            filenames = [filenames]

        read_ok = []
        for filename in filenames:
            try:
                with open(filename) as fp:
                    self.readfp(fp)
            except IOError as e:
                continue
            else:
                read_ok.append(filename)

        return read_ok

    def readfp(self, fp, *args, **kwargs):
        stream = StringIO()

        try:
            stream.name = fp.name
        except AttributeError as e:
            pass

        stream.write('[' + self._default_section + ']\n')
        stream.write(fp.read())
        stream.seek(0, 0)

        return self._read(stream, stream.name)

    def write(self, fp):
        # Write the items from the default section manually and then remove them 
        # from the data. They'll be re-added later. 
        try:
            default_section_items = self.items(self._default_section)
            self.remove_section(self._default_section)

            for (key, value) in default_section_items:
                fp.write("{0} = {1}\n".format(key, value))

            fp.write("\n")
        except NoSectionError:
            pass

        RawConfigParser.write(self, fp)

        self.add_section(self._default_section)
        for (key, value) in default_section_items:
            self.set(self._default_section, key, value)

    def _read(self, fp, fpname):
        """Parse a sectioned setup file.

        The sections in setup file contains a title line at the top,
        indicated by a name in square brackets (`[]'), plus key/value
        options lines, indicated by `name: value' format lines.
        Continuations are represented by an embedded newline then
        leading whitespace.  Blank lines, lines beginning with a '#',
        and just about everything else are ignored.
        """
        cursect = None  # None, or a dictionary
        optname = None
        lineno = 0
        e = None  # None, or an exception
        while True:
            line = fp.readline()
            if not line:
                break
            lineno += 1
            # comment or blank line?
            if line.strip() == '' or line[0] in '#;':
                continue
            if line.split(None, 1)[0].lower() == 'rem' and line[0] in "rR":
                # no leading whitespace
                continue
            # continuation line?
            if line[0].isspace() and cursect is not None and optname:
                value = line.strip()
                if value:
                    cursect[optname] = "%s\n%s" % (cursect[optname], value)
            # a section header or option header?
            else:
                # is it a section header?
                mo = self.SECTCRE.match(line)
                if mo:
                    sectname = mo.group('header')
                    if sectname in self._sections:
                        cursect = self._sections[sectname]
                    elif sectname == DEFAULTSECT:
                        cursect = self._defaults
                    else:
                        cursect = self._dict()
                        cursect['__name__'] = sectname
                        self._sections[sectname] = cursect
                    # So sections can't start with a continuation line
                    optname = None
                # no section header in the file?
                elif cursect is None:
                    raise MissingSectionHeaderError(fpname, lineno, line)
                # an option line?
                else:
                    mo = self.OPTCRE.match(line)
                    if mo:
                        optname, vi, optval = mo.group('option', 'vi', 'value')
                        if vi in ('=', ':') and ';' in optval:
                            # ';' is a comment delimiter only if it follows
                            # a spacing character
                            pos = optval.find(';')
                            if pos != -1 and optval[pos - 1].isspace():
                                optval = optval[:pos]
                        optval = optval.strip()
                        # allow empty values
                        if optval == '""':
                            optval = ''
                        optname = self.optionxform(optname.rstrip())
                        if cursect.get(optname):
                            if not isinstance(cursect[optname], list):
                                cursect[optname] = [cursect[optname]]
                            cursect[optname].append(optval)
                        else:
                            cursect[optname] = optval
                    else:
                        # a non-fatal parsing error occurred.  set up the
                        # exception but keep going. the exception will be
                        # raised at the end of the file and will contain a
                        # list of all bogus lines
                        if not e:
                            e = ParsingError(fpname)
                        e.append(lineno, repr(line))
        # if any parsing errors occurred, raise an exception
        if e:
            raise e


def split_remote_fileid(remote_file_id):
    """
    Splite remote_file_id to (group_name, remote_file_name)
    arguments:
    @remote_file_id: string
    @return tuple, (group_name, remote_file_name)
    """
    index = remote_file_id.find('/')
    if -1 == index:
        return None
    return remote_file_id[0:index], remote_file_id[(index + 1):]


def fdfs_check_file(filename):
    ret = True
    errmsg = ''
    if not os.path.isfile(filename):
        ret = False
        errmsg = '[-] Error: %s is not a file.' % filename
    elif not stat.S_ISREG(os.stat(filename).st_mode):
        ret = False
        errmsg = '[-] Error: %s is not a regular file.' % filename
    return ret, errmsg


if __name__ == '__main__':
    print(get_file_ext_name('/bc.tar.gz'))
