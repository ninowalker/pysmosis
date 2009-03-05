from ormlite import Record, ForeignKey, JSONField, TEXT, ID, FLOAT, INT, open_connection
import os, sys

class Node(Record):
    _table_ = 'nodes'
    _fields_ = (ID('id'),
                FLOAT('lat', notnull=True, index=True),
                FLOAT('lon', notnull=True, index=True),
                JSONField('tags'),
                INT('refcount'))
    
    def tuple(self):
        return (n.id, n.lat, n.lon)

class Way(Record):
    _table_ = 'ways'
    _fields_ = (ID('id'),
                JSONField('geom'),
                JSONField('nds'),
                JSONField('tags'),
                FLOAT('left'),FLOAT('bottom'),FLOAT('right'),FLOAT('top'))
    @property
    def bbox(self):
        return (self.left, self.bottom, self.right, self.top)

class NodeCoords(Record):
    _table_ = 'nodes'
    _fields_ = (ID('id'),
                FLOAT('lat', notnull=True, index=True),
                FLOAT('lon', notnull=True, index=True))
    

class WaySegment(Record):
    _fields_ = (ID('id'),
                ForeignKey('way_id', Way, 'id', 'segment_set'),
                ForeignKey('start_id', Node, 'id', 'origin_seg_set'),
                ForeignKey('end_id', Node, 'id', 'end_seg_set'),
                JSONField('geom',notnull=True),
                JSONField('nds',notnull=True),
                FLOAT('left'),FLOAT('bottom'),FLOAT('right'),FLOAT('top'))
    @property
    def bbox(self):
        return (self.left, self.bottom, self.right, self.top)

    
class OSMDB:
    def __init__(self, dbname,overwrite=False):
        if overwrite:
            try:
                os.remove( dbname )
            except OSError:
                pass
        
        if not os.path.exists(dbname):
            self.conn = open_connection(dbname)
            self.setup()
        else:
            self.conn = open_connection(dbname)
            self.setup()
        
    def setup(self):
        for n in (Node,Way,WaySegment):
            n.objects.createtable()

        c = self.conn.cursor()
        # This should be an RTree if supported
        c.execute( "CREATE INDEX ways_bbox ON ways(left, bottom, right, top)" )
        self.conn.commit()
        c.close()
        
    def populate(self, osm_filename, 
                 accept_way=lambda way: True, 
                 accept_tag=lambda k,v: k.startswith('tiger') and (None,None) or (k,v), 
                 reporter=None):
        import digest
        digest.load_osmfile(self.conn, osm_filename, accept_way, accept_tag, reporter=reporter)
        #digest.populate_way_geom(self.conn)
        #digest.segment_ways(self.conn)
        
               
    def nodes(self):
        for n in Node.objects.query():
            yield n.tuple()
        
    def node(self, id):
        return Node.objects.get(id=id).tuple()
    
    def nearest_node(self, lat, lon, range=0.005):
        q = Node.object.query(where="lat > %d AND lat < %d AND lon > %d AND lon < %d" % (lat-range, lat+range, lon-range, lon+range))
        
        dists = [(n.id, n.lat, n.lon, ((n.lat-lat)**2+(n.lon-lon)**2)**0.5) for n in q]
            
        if len(dists)==0:
            return (None, None, None, None)
            
        return min( dists, key = lambda x:x[3] )

    def nearest_of( self, lat, lon, nodes ):
        q = Node.object.query(where="id IN (%s)" % ",".join([str(x) for x in nodes]))        
        dists = [(n.id, n.lat, n.lon, ((n.lat-lat)**2+(n.lon-lon)**2)**0.5) for n in q]
        q.close()
        if len(dists)==0:
            return (None, None, None, None)
            
        return min( dists, key = lambda x:x[3] )

    def nearby_ways(self, lat, lon, range=0.005):
        q = Way.objects.query(where="left <= %d AND right >= %d and bottom <= %d and top >= %d" % (lon+range, lon-range, lat+range, lat-range) )
        
        for w in q:
            yield w
        q.close()
        
    def way(self, id):
        return Way.objects.get(id=id)
                
    def ways(self):
        for w in Way.objects.query():
            yield w
        
    def count_ways(self):
        c = self.conn.cursor()        
        c.execute( "SELECT count(*) FROM ways" )
        ret = next(c)[0]
        c.close()
        
        return ret
    
    def waysegments(self):
        for w in WaySegment.objects.query():
            yield w
    
    def nearest_way( self, x,y, range=0.001, accept_tags=lambda tags:True ):
        """returns (way_id, subsegment_num, subsegment_splitpoint, point, distance_from_point)"""
        
        lineup = []
        
        for way in self.nearby_ways( y, x, range=0.001 ):
            if accept_tags(way.tags):
                subsegment_num, subsegment_splitpoint, point, distance_from_point = closest_point_on_linestring( way.geom, (x, y) )
                lineup.append( (way, subsegment_num, subsegment_splitpoint, point, distance_from_point) )
        
        if len(lineup)==0:
            return (None, None, None, None, None)
        return min( lineup, key=lambda x:x[4] )
                
    def bounds(self):
        c = self.conn.cursor()
        c.execute( "SELECT min(left), min(bottom), max(right), max(top) FROM ways" )
        
        ret = next(c)
        c.close()
        return ret

def osm_to_osmdb(osm_filename, osmdb_filename):
    osmdb = OSMDB( osmdb_filename, overwrite=True )
    osmdb.populate( osm_filename, accept_way=lambda way: 'highway' in way.tags, reporter=sys.stdout )

if __name__=='__main__':
    from sys import argv
    
    usage = "python osmdb.py osm_filename osmdb_filename"
    if len(argv) < 3:
        print usage
        exit()

    osm_filename = argv[1]
    osmdb_filename = argv[2]
    
    osm_to_osmdb(osm_filename, osmdb_filename)
