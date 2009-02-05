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

def default_way_filter(tags):
    return True

def digest_file(osmfile, sqlite_file, 
                accept_way=default_way_filter, 
                accept_tag=default_tag_filter, 
                reporter=None):
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
        user = None 
        visible = False
        timestamp = None
        counter = 0
        tags = {}
        
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
                self.tags = {}
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
                r = accept_tag(attrs['k'],attrs['v'], self.node, self.way)
                if r:
                    self.tags[r[0]] = r[1]
                return
            elif name=='way':
                self.id = int(attrs['id'])
                self.user = attrs.get('user')
                self.visible = attrs.get('visible')=='true'
                self.timestamp = attrs['timestamp']
                self.wayrefs = []
                self.way = True
                self.tags = {}

            elif self.way and name=='nd':
                self.wayrefs.append(attrs['ref'])
            
        @classmethod
        def endElement(self,name):
            self.counter += 1
            if reporter and self.counter % 10000 == 0:
                reporter.write("Processed %d tags\n" % self.counter)
            if name == 'node':
                if len(self.tags):
                    cur.executemany('INSERT INTO node_tags VALUES (?,?,?)', [(self.id,k,v) for k,v in self.tags.items()])
                    self.tags = {}
                conn.commit()
                self.node = False
                return
            elif name == 'way':
                #if self.id == 16752188:
                #    print self.tags
                #    #assert False
                if not accept_way(self.tags):
                    #print "Skiping way", self.tags
                    self.way = False
                    self.tags = {}
                    return
                cur.execute("INSERT into ways VALUES (?,?,?,?)", (self.id,
                                                                  self.user,
                                                                  self.visible,
                                                                  self.timestamp))
                #if self.id == 16752188:
                #    print "Crap..."
                 #   assert False
                if len(self.tags):
                    cur.executemany('INSERT INTO way_tags VALUES (?,?,?)', [(self.id,k,v) for k,v in self.tags.items()])
                 #   self.tags = {}
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
    
def segment_ways(conn, reporter=None):
    cur = conn.cursor()
    
    counter = 0
    totalways = 0
    if reporter:
        waycount = cur.execute("select count(id) from ways").fetchone()[0]
        reporter.write("Segmenting %d ways\n" % waycount)
    seq = []
    seq_num = 0
    way_id = None
    start_node = None
    end_node = None

    l = float('inf')
    b = float('inf')
    r = -float('inf')
    t = -float('inf')
    tup = None
    
    for wid, nid, lat, lng, cnt in conn.cursor().execute("select ways.id, nodes.id, nodes.latitude, nodes.longitude, node_refs.cnt from ways "
                                               "join way_nodes on ways.id = way_nodes.id "
                                               "join nodes on nodes.id = way_nodes.node_id "
                                               "join node_refs on way_nodes.node_id = node_refs.id "
                                               "order by ways.id, way_nodes.sequence_id"):

        tup = None
        end_node = nid
        if wid != way_id:
            if way_id and len(seq) > 1:
                tup = [way_id,seq_num,start_node,end_node, seq, l,b,r,t]
                l = float('inf')
                b = float('inf')
                r = -float('inf')
                t = -float('inf')
            start_node = nid
            seq_num = 0
            way_id = wid
            seq = [(lng, lat)]
        elif cnt > 1: # intersection
            seq.append((lng, lat))
            tup = [way_id,seq_num,start_node,end_node,seq, l,b,r,t]
            start_node = nid
            seq_num += 1
            seq = [(lng, lat)]
        else:
            seq.append((lng, lat))

        l = min(l,lng)
        b = min(b,lat)
        r = max(r,lng)
        t = max(t,lat)
        
        if tup:
            tup[4] = "SRID=4326;LINESTRING(%s)" % ",".join(["%f %f" % c for c in tup[4]])
            #print "Tup", tup
            cur.execute("INSERT into way_segments (way_id,sequence_id,start_node_id,end_node_id,wkt,left,bottom,right,top) VALUES (?,?,?,?,?,?,?,?,?)", tup)

    # the  final one:
    #print "Tup", tup
    tup = (way_id, seq_num, start_node, end_node, 
         "SRID=4326;LINESTRING(%s)" % ",".join(["%f %f" % c for c in seq]),
         l,b,r,t)
    cur.execute("INSERT into way_segments (way_id,sequence_id,start_node_id,end_node_id,wkt,left,bottom,right,top) VALUES (?,?,?,?,?,?,?,?,?)", tup)

    if reporter:
        reporter.write("Derived %d segments\n" % cur.execute("select count(*) from way_segments").fetchone())

def purge_intermediate_nodes(conn, reporter=None):
    if reporter: reporter.write("Deleting intermediate nodes...\n")
    c = conn.cursor()
    c.execute("DELETE from nodes where id not in (select start_node_id from way_segments) and id not in (select end_node_id from way_segments)")
    c.execute("DELETE from node_tags where id not in (select start_node_id from way_segments) and id not in (select end_node_id from way_segments)")
    if reporter: reporter.write("Deleted.\n")

def purge_node_refs(conn, reporter=None):
    if reporter: reporter.write("Deleting node refs...\n")
    c = conn.cursor()
    c.execute("DROP TABLE node_refs;")
    conn.commit()
    if reporter: reporter.write("Deleted.\n")

        
def create_node_rtree_index(conn, reporter=None):
    if reporter: reporter.write("Building node Rtree...\n")
    try:
        c = conn.cursor()
        c.execute("CREATE VIRTUAL TABLE node_latlng_rtree_idx USING rtree(id, lng, lat)")
        c.execute("INSERT INTO node_latlng_rtree_idx SELECT id, longitude, latitude FROM nodes")
        conn.commit()
    except:
        raise SQLiteFeatureNotSupported("Unable to create an r*tree index. " + \
                                        "You need to recompile sqlite3 to use this functionality. http://www.sqlite.org/rtree.html")
    if reporter: reporter.write("Done.\n")

def create_ways_rtree_index(conn, reporter=None):
    if reporter: reporter.write("Building way segment Rtree...\n")
    try:
        c = conn.cursor()
        c.execute("CREATE VIRTUAL TABLE way_segments_rtree_idx USING rtree(id, left, bottom, right, top)")
        c.execute("INSERT INTO way_segments_rtree_idx SELECT id, left, bottom, right, top FROM way_segments")
        conn.commit()
    except:
        raise SQLiteFeatureNotSupported("Unable to create an r*tree index. " + \
                                        "You need to recompile sqlite3 to use this functionality. http://www.sqlite.org/rtree.html")
    if reporter: reporter.write("Done.\n")

class SQLiteFeatureNotSupported(Exception):
    pass
