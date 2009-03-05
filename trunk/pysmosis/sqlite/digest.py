import xml.sax
#import bsddb3 as bsddb
import sqlite3
import os
import bz2
from ORM import Way, WaySegment, Node
    
def load_osmfile(conn, osmfile, 
              accept_way=lambda way: 'highway' in way.tags,
              accept_tag=lambda k,v: k.startswith('tiger') and (None,None) or (k,v),
              reporter=None):
        
    cur = conn.cursor()
    cur.execute('PRAGMA synchronous=OFF;')
    
    class FastOSMHandler(xml.sax.ContentHandler):
        object = None
        is_way = False
        counter = 0
        
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
            if name=='node':
                self.object = Node(id=int(attrs['id']),
                                   lat=float(attrs['lat']),
                                   lon=float(attrs['lon']),
                                   tags={},
                                   refcount=0)
                self.is_way = False
                return
            
            elif name=='tag':
                k,v = accept_tag(attrs['k'],attrs['v'])
                #print k,v
                if k:
                    self.object.tags[k] = v
                return

            elif name=='way':
                self.object = Way(id=int(attrs['id']), nds=[], geom=[], tags={})
                self.is_way = True
                return 
            
            elif self.is_way and name=='nd':
                self.object.nds.append(attrs['ref'])
                return
            
        @classmethod
        def endElement(self,name):
            self.counter += 1
            if reporter and self.counter % 10000 == 0:
                reporter.write("Processed %d tags\n" % self.counter)
            if name == 'node':
                self.object.tags = self.object.tags 
                self.object.create(autocommit=True)
                self.object = None
                return

            elif name == 'way':
                if accept_way(self.object):
                    #print self.object._fields_
                    self.object.tags = self.object.tags 
                    self.object.create(autocommit=False)
                    cur.execute("UPDATE nodes set refcount = refcount + 1 where id in (%s)" % ",".join(self.object.nds))                        
                    conn.commit()
                self.object = None
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
        waycount = cur.execute("select count(id) from way").fetchone()[0]
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
