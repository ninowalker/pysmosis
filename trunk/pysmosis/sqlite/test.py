import digest
import os

digest.VERBOSE = True

class BaseTest:
    file1 = os.path.join(os.path.dirname(__file__), "..", "test", "test_file.osm")

    def test_basic(self):
        def my_tag_filter(k, v, is_node, is_way):
            if is_node or k not in ('highway', 'name'):
                return None
            # return a tuple of (key, value) or None, if you want to ignore the tag
            return k, v
        
        conn = digest.digest_file(self.file1, ":memory:", my_tag_filter)
        digest.segment_ways(conn)
        c = conn.cursor()
        assert 2967 == c.execute("select count(*) from nodes").fetchone()[0]
        assert 319 == c.execute("select count(*) from ways").fetchone()[0]
        assert 1218 == c.execute("select count(*) from way_segments").fetchone()[0]
        
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
 