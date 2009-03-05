try:
    import sqlite3
except ImportError:
    from pysqlite2 import dbapi2 as sqlite3

import sys
import simplejson as json
import cPickle as pickle
from cStringIO import StringIO
import pdb

connection = None
verbose = False
reporter = sys.stdout

def open_connection(filename, **kwargs):
    global connection 
    connection = sqlite3.connect(filename, **kwargs)
    return connection

def record_factory(cls, use_dict=False):
    if use_dict:
        def _fact(cursor, row):
            kwargs = {}
            for i,col in enumerate(cursor.description):
                kwargs[cls._mappings_[col[0]]] = row[i]
            return cls(**kwargs)
    else:
        def _fact(cursor, row):
            return cls(**dict([(k,v) for k,v in zip(cls._mappings_.values(), row)]))
    return _fact

class NoPrimaryKeyError(Exception):
    pass

class FieldBase(object):
    def __init__(self, fieldname, default=None, index=False, notnull=False, primary_key=False):
        self.fieldname = fieldname
        self.default = default
        self.index = index
        self.notnull = notnull
        self.primary_key = primary_key
    
    def contribute(self, new_cls):
        setattr(new_cls, self.fieldname, self.default)
        return self.fieldname
    
    def lite_type(self):
        return "TEXT"
    
    def lite_column_definition(self):
        return "%s %s%s%s" % (self.fieldname, 
                              self.lite_type(),
                              self.default and " DEFAULT " + self.default or "", 
                              self.notnull and " NOT NULL" or "")


class TEXT(FieldBase):
    pass
    
class INT(FieldBase):
    def lite_type(self):
        return "INTEGER"

class FLOAT(FieldBase):
    def lite_type(self):
        return "FLOAT"

class ID(FieldBase):
    def __init__(self, fieldname, auto_increment=True, **kwargs):
        kwargs['primary_key'] = True
        super(ID, self).__init__(fieldname, **kwargs)
        self.auto_increment = auto_increment
        
    def lite_type(self):
        return "INTEGER PRIMARY KEY" + (self.auto_increment and " AUTOINCREMENT" or "")
    
    
class ForeignKey(FieldBase):
    def __init__(self, this_key, other_cls, other_key, related_name, 
                 index=True, notnull=False, litetype="INTEGER"):
        self.this_key = this_key
        self.other_cls = other_cls
        self.other_key = other_key
        self.related_name = related_name
        self._lite_type = litetype
        super(ForeignKey, self).__init__(this_key, index=index, notnull=notnull)

    def lite_type(self):
        return self._lite_type

    def contribute(self, new_class):
        def _getfkey(o):
            v = getattr(o, self.this_key)
            if v != None:
                return self.other_cls.objects.query(**{self.other_key:v})
            return None
        
        def _getrelatedset(o):
            return new_class.objects.query(**{self.this_key:getattr(o,self.other_key)})
        
        setattr(new_class, self.this_key[0:-3], property(_getfkey))
        setattr(self.other_cls, self.related_name, property(_getrelatedset))
        return self.this_key


class JSONField(FieldBase):
    def contribute(self, new_cls):
        def _set(o, value):
            if value != None:
                setattr(o, "__%s_raw" % self.fieldname, json.dumps(value))
            else:
                setattr(o, "__%s_raw" % self.fieldname, value)
            setattr(o, "__%s_cache" % self.fieldname, value)

        def _get(o):
            v = getattr(o, "__%s_cache" % self.fieldname)
            if v != None: 
                return v
            v = getattr(o, "__%s_raw" % self.fieldname)
            if v != None:
                v = json.loads(v)
                setattr(o, "__%s_cache" % self.fieldname, v)
            return v
                    
        setattr(new_cls, "__%s_cache" % self.fieldname, None)
        setattr(new_cls, "__%s_raw" % self.fieldname, None)
        setattr(new_cls, self.fieldname, property(_get,_set))
        return "__%s_raw" % self.fieldname

class PickleField(FieldBase):
    def contribute(self, new_cls):
        def _set(o, value):
            if value != None:
                setattr(o, "__%s_raw" % self.fieldname, pickle.dumps(value))
            else:
                setattr(o, "__%s_raw" % self.fieldname, value)
            setattr(o, "__%s_cache" % self.fieldname, value)

        def _get(o):
            v = getattr(o, "__%s_cache" % self.fieldname)
            if v != None: 
                return v
            v = getattr(o, "__%s_raw" % self.fieldname)
            if v != None:
                v = pickle.loads(str(v))
                setattr(o, "__%s_cache" % self.fieldname, v)
            return v
        
        setattr(new_cls, "__%s_cache" % self.fieldname, None)
        setattr(new_cls, "__%s_raw" % self.fieldname, None)
        setattr(new_cls, self.fieldname, property(_get,_set))
        return "__%s_raw" % self.fieldname
    
class RecordBase(type):
    #__metaclass__ = object 
    def __new__(cls, name, bases, attrs):
        obj_new = super(RecordBase, cls).__new__
        parents = [b for b in bases if hasattr(b,'_fields_')]
                
        new_class = obj_new(cls, name, bases, attrs)
        for obj_name, obj in attrs.items():                 
            setattr(new_class, obj_name, obj)
            

        mappings = {}
        for p in parents:
            mappings.update(p._mappings_)
        fields = list(attrs['_fields_'])
        fielddefs = []
        primary_keys = []
        for f in fields:
            if not isinstance(f, FieldBase):
                f = FieldBase(f)
            
            if f.primary_key:
                primary_keys.append(f)
            mappings[f.fieldname] = f.contribute(new_class)
            fielddefs.append(f)
            
        
        setattr(new_class,'_mappings_',mappings)
        setattr(new_class,'_fielddefs_',fielddefs)
        if not len(primary_keys):
            setattr(new_class,'_primary_keys_',None)
        else:
            setattr(new_class,'_primary_keys_',tuple(primary_keys))

        if '_table_' not in attrs:
            setattr(new_class, '_table_', name.lower())
            
        if 'objects' not in attrs:
            setattr(new_class, 'objects', BaseManager())
        else:
            setattr(new_class, 'objects', attrs['objects'])
            
        new_class.objects.rclass = new_class
        return new_class


class BaseManager(object):
    def __init__(self, rclass=None):
        # this is set by the __new__ method of class
        self.rclass = rclass
    
    def createtable(self):
        c = connection.cursor()
        c.execute(self.tabledef())
        c.executescript(self.indicesdef())
        connection.commit()
    
    def tabledef(self):
        pkeys = [p.fieldname for p in filter(lambda f: not isinstance(f, ID), self.rclass._primary_keys_)]
        return "CREATE TABLE %s (%s%s);" % (self.rclass._table_,
                                            ",\n".join([f.lite_column_definition() for f in self.rclass._fielddefs_]),
                                            len(pkeys) and \
                                            ",\nPRIMARY KEY (%s)" % ", ".join(pkeys) or "") 
    
    def indicesdef(self):
        fields = [p for p in filter(lambda f: f.index and not f.primary_key, 
                                    self.rclass._fielddefs_)]
        ind = []
        for f in fields:
            ind.append("CREATE INDEX %s_%s_indx ON %s(%s);" % (self.rclass._table_,
                                                              f.fieldname,
                                                              self.rclass._table_,
                                                              f.fieldname)) 
        return "\n".join(ind)
        
    def get(self, **kwargs):
        return self.query(**kwargs).fetchone()
    
    @classmethod    
    def query(cls, **kwargs):
        c = connection.cursor()
        query = ["1"]
        args = []
        if kwargs:
            for k,v in kwargs.items():
                if k == 'where':
                    query.append(v)
                else:
                    query.append("%s=?" % cls._mappings_[k])
                    args.append(v)
        q = "SELECT %s from %s where %s" % (",".join(cls._mappings_.keys()), cls._table_, 
                                            " AND ".join(query))
        if verbose: reporter.write("Query: %s\n" % q)
        c.execute(q, args)
        c.row_factory = record_factory(cls)
        return c
    
    def query(self, **kwargs):
        c = connection.cursor()
        query = ["1"]
        args = []
        if kwargs:
            for k,v in kwargs.items():
                if k == 'where':
                    query.append(v)
                else:
                    query.append("%s=?" % self.rclass._mappings_[k])
                    args.append(v)
        q = "SELECT %s from %s where %s" % (",".join(self.rclass._mappings_.keys()), 
                                            self.rclass._table_, " AND ".join(query))
        if verbose: reporter.write("Query: %s\n" % q)
        c.execute(q, args)
        c.row_factory = record_factory(self.rclass)
        return c

    def cursor(self):
        c = connection.cursor()
        c.row_factory = record_factory(self.rclass, use_dict=True)
        return c
        
    def join(self, othercls, on, **kwargs):
        c = connection.cursor()
        query = ["1"]
        if kwargs:
            for k,v in kwargs:
                if k == 'where':
                    query.append(v)
                else:
                    if '__' in k:
                        k = othercls._table_ + k[k.find('__')+2:]
                    query.append("%s=%s" % (k,v))
        q = "SELECT %s from %s join %s on %s where %s" % (",".join(self.rclass._mappings_.values()), 
                                                          self.rclass._table_, " AND ".join(query))
        if verbose: reporter.write("Query: %s\n" % q)
        c.execute(q)
        c.row_factory = record_factory(self.rclass)
        return c


class Record(object):
    __metaclass__ = RecordBase
    _fields_ = []
    def __init__(self, **kwargs):
        super(Record, self).__init__()
        if kwargs:
            for k,v in kwargs.items():
                #if k in self._mappings_:
                setattr(self, k, v)
                    
    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__, 
                           ", ".join(["%s=%s" % (f, getattr(self,f)) for f in self._mappings_.keys()]))

    def __getitem__(self, key):
        if key in self._mappings_:
            return getattr(self, key)
        raise KeyError(key)
    
    def items(self):
        for f in self._mappings_.keys():
            yield f, getattr(self, f)
            
    def keys(self):
        return self._mappings_.keys()
    
    def create(self, autocommit=True, get_rowid=False):
        c = connection.cursor()
        #pdb.set_trace()
        q = "INSERT INTO %s(%s) VALUES (%s)" % (self._table_, 
                                                      ",".join(self._mappings_.keys()), 
                                                      ",".join(["?" for f in self._mappings_.keys()]))
        if verbose: reporter.write("Create: %s\n" % q)
        #print [getattr(self, f) for f in self._mappings_.values()]
        c.execute(q, [getattr(self, f) for f in self._mappings_.values()])
        if autocommit or get_rowid:
            connection.commit()
            return c.lastrowid
            
    def save(self):
        if not len(self._primary_keys_):
            raise NoPrimaryKeyError(self.__class__.__name__)
        c = connection.cursor()
        c.execute("UPDATE %s set %s=? where %s=?" % (self._table_, 
                                                   "=?,".join(self._mappings_.keys()),
                                                   "=? and ".join([f.fieldname for f in self._primary_keys_]), 
                                                   [getattr(self, f) for f in self._mappings_.values()]))
    def delete(self):
        if not len(self._primary_keys_):
            raise NoPrimaryKeyError(self.__class__.__name__)
        c = connection.cursor()
        q = "DELETE from %s where %s=?" % (self._table_, 
                                           "=? and ".join([f.fieldname for f in self._primary_keys_]))
        
        if verbose: reporter.write("Delete: %s\n" % q)
        c.execute(q, [getattr(self, f.fieldname) for f in self._primary_keys_])

            
def test():
    class Foo(Record):
        _fields_ = (ID('id'),INT('moo'), PickleField('pickle'))
        #_primary_key_ = ('id',)

    class Bar(Record):
        _fields_ = (ID('id'), 
                    ForeignKey('foo_id', Foo, 'id', 'bar_set'), 
                    JSONField('json_array', notnull=False))
        #_primary_key_ = ('id',)

    open_connection(":memory:")
    c = connection.cursor()
    Foo.objects.createtable()
    Bar.objects.createtable()
    #c.execute("create table foo (id integer primary key, moo integer, pickle TEXT)")
    #c.execute("create table bar (id integer primary key, foo_id integer, json_array TEXT)")
    #c.execute("create table foo (id integer primary key AUTOINCREMENT, moo integer, pickle TEXT)")
    #c.execute("create table bar (id integer primary key AUTOINCREMENT, foo_id integer, json_array TEXT)")
    
    #print dir(Foo())
    f = Foo(id=1, moo=2, pickle={'a':'b'})
    f.create()
    assert 'a' in Foo.objects.get(id=1).pickle

    Foo(id=2,cow=3, moo=2).create()
    b = Bar(id=1,foo_id=1)
    b.create()
    b = Bar(id=2,foo_id=1,json_array=[1,2,3])
    b.create()
    for o in Foo.objects.query():
        #print "Fetch:", o
        assert o.moo == 2
        assert o.id != 3
    
    assert len(b.foo.fetchall()) == 1
    #print ",".join(map(str,f.bar_set.fetchall()))
    assert len(f.bar_set.fetchall()) == 2
    b = Bar.objects.get(id=2)
    assert b.foo_id == 1
    #print c.execute("select * from bar").fetchall()
    #print b.json_array, len(b.json_array), type(b.json_array)
    assert len(b.json_array) == 3
    assert sum(b.json_array) == 6
    
    for b in Bar.objects.cursor().execute("select foo_id from bar"):
        #print b
        assert b.id == None
        assert b.foo_id == 1
        
    b = Bar.objects.get(id=2)
    b.delete()
    assert len(Bar.objects.query().fetchall()) == 1

    f = Foo(moo=2, pickle={'a':'b'})
    id = f.create(get_rowid=True)
    print id 
    assert id == 3
    
    class Dud(Record):
        _fields_ = (TEXT('a',primary_key=True),TEXT('b',primary_key=True))
    
    print Bar.objects.tabledef()
    print Dud.objects.tabledef()
    
    Dud.objects.createtable()
    
    Dud(a="a",b="b").create()
    try:
        Dud(a="a",b="b").create()
        assert False
    except:
        assert True
        
    print "Passed"
    
    fields = 0 
    print "Iterating..."
    for k, v in f.items():
        fields += 1
        print k, v
        
    assert fields == len(Foo._mappings_)
    
    #print "F bar_set:",f.bar_set.fetchall()
    #for o in Foo.join(Bar, 'id = foo_id'):
    #    print "Join:", o
        
if __name__  == '__main__':
    test()
    
