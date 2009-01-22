import xml.sax
#import bsddb3 as bsddb
import sqlite3
import os
import bz2

DBTEMPLATE=os.path.join(os.path.dirname(__file__), "osm.sqlite.sql")
VERBOSE = False

def create_db(sqlite_file):
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    dbtemplate = open(DBTEMPLATE).read()
    c.executescript(dbtemplate)
    return conn

def open_db(sqlite_file):
    return sqlite3.connect(sqlite_file)
    
def default_tag_filter(k, v, is_node, is_way):
    # return a tuple of (key, value) or None, if you want to ignore the tag
    return k, v
    
def digest_file(osmfile, sqlite_file, tag_filter=default_tag_filter):
    if os.path.exists(sqlite_file):
        conn = open_db(sqlite_file)
    else:
        conn = create_db(sqlite_file)
        
    cur = conn.cursor()
    cur.execute('PRAGMA synchronous=OFF;')
    
    class FastOSMHandler(xml.sax.ContentHandler):
        id = None
        way = False
        node = True
        waytype = None
        wayrefs = None
        counter = 0
        tags = []
        
        @classmethod
        def setDocumentLocator(self,loc):
            pass
        
        @classmethod
        def startDocument(self):
            pass
            
        @classmethod
        def endDocument(self):
            pass
        
        @classmethod
        def characters(self, chars):
            pass
            
        @classmethod
        def startElement(self, name, attrs):
            if name=='bounds':
                cur.execute("INSERT into bounds VALUES (?,?,?,?)", (attrs['minlat'],attrs['minlon'],
                                                             attrs['maxlat'],attrs['maxlon']))
                conn.commit()
                return
            elif name=='node':
                self.id = int(attrs['id'])
                self.tags = []
                self.node = True
                cur.execute("INSERT into nodes VALUES (?,?,?,?,?,?)", (self.id,
                                                                       float(attrs['lat']),
                                                                       float(attrs['lon']),
                                                                       attrs.get('user'),
                                                                       attrs.get('visible')=='true',
                                                                       attrs['timestamp']))
                
                cur.execute("INSERT into node_refs VALUES (?,?)", (self.id, 0))
                return
            elif name=='tag':
                r = tag_filter(attrs['k'],attrs['v'], self.node, self.way)
                if r:
                    self.tags.append(r)
                return
            elif name=='way':
                self.id = int(attrs['id'])
                cur.execute("INSERT into ways VALUES (?,?,?,?)", (self.id,
                                                                  attrs.get('user'),
                                                                  attrs.get('visible')=='true',
                                                                  attrs['timestamp']))
                self.wayrefs = []
                self.way = True
                self.tags = []

            elif self.way and name=='nd':
                self.wayrefs.append(attrs['ref'])
            
        @classmethod
        def endElement(self,name):
            self.counter += 1
            if VERBOSE and self.counter % 10000 == 0:
                print("Processed %d tags" % self.counter)
            if name == 'node':
                if len(self.tags):
                    cur.executemany('INSERT INTO node_tags VALUES (?,?,?)', [(self.id,)+ t for t in self.tags])
                    self.tags = []
                conn.commit()
                self.node = False
                return
            elif name == 'way':
                if len(self.tags):
                    cur.executemany('INSERT INTO way_tags VALUES (?,?,?)', [(self.id,)+ t for t in self.tags])
                    self.tags = []
                if len(self.wayrefs):
                    cur.executemany('INSERT INTO way_nodes VALUES (?,?,?)', 
                                    [(self.id,nd,i) for i,nd in enumerate(self.wayrefs)])
                    cur.execute('UPDATE node_refs SET cnt = cnt + 1 WHERE id IN (%s)' % ",".join(self.wayrefs))
                conn.commit()
                self.way = False
                return

    if type(osmfile) == str and osmfile.endswith("bz2"):
        osmfile = bz2.BZ2File(osmfile,"r")
    xml.sax.parse(osmfile, FastOSMHandler)
    return conn
    
def segment_ways(conn):
    cur = conn.cursor()
    
    counter = 0
    totalways = 0
    if VERBOSE:
        waycount = cur.execute("select count(id) from ways").fetchone()[0]
        print("processing %d ways" % waycount)
    seq = []
    seq_num = 0
    way_id = None
    start_node = None
    end_node = None
    
    for wid, nid, lat, lng, cnt in conn.cursor().execute("select ways.id, nodes.id, nodes.latitude, nodes.longitude, node_refs.cnt from ways "
                                               "join way_nodes on ways.id = way_nodes.id "
                                               "join nodes on nodes.id = way_nodes.node_id "
                                               "join node_refs on way_nodes.node_id = node_refs.id "
                                               "order by ways.id, way_nodes.sequence_id"):
        t = None
        end_node = nid
        if wid != way_id:
            if way_id and len(seq) > 1:
                t = [way_id,seq_num,start_node,end_node, seq]
            start_node = nid
            seq_num = 0
            way_id = wid
            seq = [(lng, lat)]
        elif cnt > 1: # intersection
            seq.append((lng, lat))
            t = [way_id,seq_num,start_node,end_node,seq]
            start_node = nid
            seq_num += 1
            seq = [(lng, lat)]
        else:
            seq.append((lng, lat))
        
        if t:
            t[-1] = "SRID=4326;LINESTRING(%s)" % ",".join(["%f %f" % c for c in t[4]])
            cur.execute("INSERT into way_segments VALUES (?,?,?,?,?)", t)

    # the  final one:
    t = (way_id, seq_num, start_node, end_node, "SRID=4326;LINESTRING(%s)" % ",".join(["%f %f" % c for c in seq]))
    if VERBOSE:
        cur.execute("INSERT into way_segments VALUES (?,?,?,?,?)", t)
        print("Derived %d segments" % cur.execute("select count(*) from way_segments").fetchone())
 
