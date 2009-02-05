import digest
import os
import sys

digest.VERBOSE = True

class TestBase:
    file1 = os.path.join(os.path.dirname(__file__), "..", "test", "test_file.osm")

    def test_basic(self):
        print "Digesting..."
        def my_tag_filter(k, v, is_node, is_way):
            if is_node or k not in ('highway', 'name'):
                return None
            # return a tuple of (key, value) or None, if you want to ignore the tag
            return k, v
        def my_way_filter(tags):
            #return True
            return 'highway' in tags
        
        db = ":memory:"
        #db = "t.sqlite"
        conn = digest.digest_file(self.file1, db, 
                                  accept_tag=my_tag_filter, 
                                  accept_way=my_way_filter, reporter=sys.stdout)
        c = conn.cursor()
        assert 2967 == c.execute("select count(*) from nodes").fetchone()[0]
        assert 309 == c.execute("select count(*) from ways").fetchone()[0]
        assert 0 == c.execute("select count(*) from ways where id not in (select id from way_tags where k = 'highway')").fetchone()[0]
        assert 309 == c.execute("select count(*) from ways join way_tags on ways.id = way_tags.id and k = 'highway'").fetchone()[0]

        digest.segment_ways(conn, reporter=sys.stdout)
        assert 1207 == c.execute("select count(*) from way_segments").fetchone()[0]
        digest.purge_intermediate_nodes(conn, reporter=sys.stdout)
        digest.purge_node_refs(conn, reporter=sys.stdout)
        assert 708 == c.execute("select count(*) from nodes").fetchone()[0]
        
        digest.create_node_rtree_index(conn, reporter=sys.stdout)
        digest.create_ways_rtree_index(conn, reporter=sys.stdout)
        
        assert 1207 == c.execute("select count(*) from way_segments_rtree_idx").fetchone()[0]
        assert 708 == c.execute("select count(*) from node_latlng_rtree_idx").fetchone()[0]
        
        assert 1207 == c.execute("select count(*) from way_segments, way_segments_rtree_idx i where i.id = way_segments.id and i.left > -123 and i.bottom > 36 and i.right < -120 and i.top < 38").fetchone()[0]
        
        
if __name__ == '__main__':
    BaseTest().test_basic()
"""
for n in c.execute('select n.*,r.cnt from nodes n, node_refs r where n.id = r.id and r.cnt > 1'):
    print(n)

for s in c.execute('select * from way_segments'):
    print("Seg", s)
for n in c.execute('select * from node_tags'):
    print(n)

for n in c.execute('select * from way_tags'):
    print(n)
"""   
 