import digest

def my_tag_filter(k, v, is_node, is_way):
    if is_node or k not in ('highway', 'name'):
        return None
    # return a tuple of (key, value) or None, if you want to ignore the tag
    return k, v
    

#db = ":memory:"
#db = "map_bernal.osm.sqlite"
file = "map_bernal.osm.xml"
file = "/opt/geodata/osm/new_york.osm.bz2"
conn = digest.digest_file(file, file + ".sqlite", my_tag_filter)
digest.segment_ways(conn)
c = conn.cursor()
for n in c.execute('select n.*,r.cnt from nodes n, node_refs r where n.id = r.id and r.cnt > 1'):
    print(n)

for s in c.execute('select * from way_segments'):
    print("Seg", s)
"""
for n in c.execute('select * from node_tags'):
    print(n)

for n in c.execute('select * from way_tags'):
    print(n)
"""   
 