try:
    import sqlite3
except ImportError:
    from pysqlite2 import dbapi2 as sqlite3

import sys
import simplejson as json
import cPickle as pickle
from cStringIO import StringIO


connection = None
verbose = True
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

class FieldBase(object):
    def __init__(self, fieldname):
        self.fieldname = fieldname
    
    def contribute(self, new_cls):
        setattr(new_cls, self.fieldname, None)
        return self.fieldname

class ForeignKey(FieldBase):
    def __init__(self, this_key, other_cls, other_key, related_name):
        self.this_key = this_key
        self.other_cls = other_cls
        self.other_key = other_key
        self.related_name = related_name
        super(ForeignKey, self).__init__(this_key)

    def contribute(self, new_class):
        def _getfkey(o):
            v = getattr(o, self.this_key)
            if v != None:
                return self.other_cls.query(**{self.other_key:v})
            return None
        
        def _getrelatedset(o):
            return new_class.query(**{self.this_key:getattr(o,self.other_key)})
        
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
            
        #fields = []
        mappings = {}
        for p in parents:
            mappings.update(p._mappings_)
            #fields.extend(list(getattr(p, '_fields_')))
        fields = list(attrs['_fields_'])
        #mappings.update(attrs['_fields_'])
        for f in fields:
            if isinstance(f, FieldBase):
                mappings[f.fieldname] = f.contribute(new_class)
            else:
                mappings[f] = f
                setattr(new_class, f, None)
        setattr(new_class,'_mappings_',mappings)

        if '_table_' not in attrs:
            setattr(new_class, '_table_', name.lower())
        """
        for fkey in attrs.get('_foreign_keys_', []):
            def _getfkey(o):
                v = getattr(o, fkey.this_key)
                if v != None:
                    return fkey.other_cls.query(**{fkey.other_key:v})
                return None
            
            def _getrelatedset(o):
                return new_class.query(**{fkey.this_key:getattr(o,fkey.other_key)})
            
            setattr(new_class, fkey.this_key[0:-3], property(_getfkey))
            setattr(fkey.other_cls, fkey.related_name, property(_getrelatedset))
        """ 
        return new_class

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
        c = connection.cursor()
        c.execute("UPDATE %s set %s=? where %s=?" % (self._table_, 
                                                   "=?,".join(self._mappings_.keys()),
                                                   "=? and ".join(self._primary_key_), 
                                                   [getattr(self, f) for f in self._mappings_.values()]))
    
    def delete(self):
        c = connection.cursor()
        q = "DELETE from %s where %s=?" % (self._table_, 
                                           "=? and ".join(self._primary_key_))
        
        if verbose: reporter.write("Delete: %s\n" % q)
        c.execute(q, [getattr(self, f) for f in self._primary_key_])
    
    @classmethod
    def get(cls, **kwargs):
        return cls.query(**kwargs).fetchone()
    
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

    @classmethod
    def cursor(cls):
        c = connection.cursor()
        c.row_factory = record_factory(cls, use_dict=True)
        return c
        
    @classmethod
    def join(cls, othercls, on, **kwargs):
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
        q = "SELECT %s from %s join %s on %s where %s" % (",".join(cls._mappings_.values()), cls._table_, " AND ".join(query))
        if verbose: reporter.write("Query: %s\n" % q)
        c.execute(q)
        c.row_factory = record_factory(cls)
        return c
        

def test():
    class Foo(Record):
        _fields_ = ('id','moo', PickleField('pickle'))
        _primary_key_ = ('id',)

    class Bar(Record):
        _fields_ = ('id', 
                    ForeignKey('foo_id', Foo, 'id', 'bar_set'), 
                    JSONField('json_array'))
        _primary_key_ = ('id',)

    open_connection(":memory:")
    c = connection.cursor()
    c.execute("create table foo (id integer primary key AUTOINCREMENT, moo integer, pickle TEXT)")
    c.execute("create table bar (id integer primary key AUTOINCREMENT, foo_id integer, json_array TEXT)")
    
    #print dir(Foo())
    f = Foo(id=1, moo=2, pickle={'a':'b'})
    f.create()
    assert 'a' in Foo.get(id=1).pickle

    Foo(id=2,cow=3, moo=2).create()
    b = Bar(id=1,foo_id=1)
    b.create()
    b = Bar(id=2,foo_id=1,json_array=[1,2,3])
    b.create()
    for o in Foo.query():
        #print "Fetch:", o
        assert o.moo == 2
        assert o.id != 3
    
    assert len(b.foo.fetchall()) == 1
    #print ",".join(map(str,f.bar_set.fetchall()))
    assert len(f.bar_set.fetchall()) == 2
    b = Bar.get(id=2)
    assert b.foo_id == 1
    #print c.execute("select * from bar").fetchall()
    #print b.json_array, len(b.json_array), type(b.json_array)
    assert len(b.json_array) == 3
    assert sum(b.json_array) == 6
    
    for b in Bar.cursor().execute("select foo_id from bar"):
        #print b
        assert b.id == None
        assert b.foo_id == 1
        
    b = Bar.get(id=2)
    b.delete()
    assert len(Bar.query().fetchall()) == 1

    f = Foo(moo=2, pickle={'a':'b'})
    id = f.create(get_rowid=True)
    print id 
    assert id == 3
    
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
    
