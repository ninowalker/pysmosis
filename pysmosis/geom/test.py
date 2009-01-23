import pyximport; pyximport.install()
import linearref as l  

def test_basic():
    assert int(157249.057369) == int(l.distance_earth(0,0,1,1))
    p = l.GPoint(0,0); p2 = l.GPoint(1,1)
    assert int(157249.057369) == int(p.distance_pt(p2))
    s = l.Line(p, p2)
    c = s.closest_pt(l.GPoint(0.5,0.5))
    print c
    assert c.x == 0.5 and c.y == 0.5
    c = s.closest_pt(l.GPoint(0,0.5))
    assert c.x == 0.25 and c.y == 0.25
    assert int(39312.8257487) == int(s.distance_pt(l.GPoint(0,0.5)))
    
def test_linestring():
    def tester(*args):
        s = l.LineString(*args)
        print "s len", len(s)
        assert len(s) == 2
        print s.pts[0]
        print s.length()
        assert s.length() == 2**0.5
    
    tester((0,0),(1,1))
    tester([(0,0),(1,1)])
    tester(((0,0),(1,1)))
    
    l1 = l.LineString([(0,0), (1,1)])
    l2 = l.LineString([(1,0), (1,1)])
    assert l1.distance_to_line(l2) == 0
    l2 = l.LineString([(2,1), (100,100)])
    print l1.distance_to_line(l2)
    assert l1.distance_to_line(l2) == 1

    l1 = l.LineString([(0,0), (1,1), (2,2)])
    assert l1.distance_pt(l.Point(1,1)) == 0
    assert l1.distance_pt(l.Point(3,3)) == 2**0.5
    assert l1.distance_pt(l.Point(0.5,0.5)) == 0
    assert l1.closest_pt(l.Point(0,0.5)).x == 0.25
    assert l1.length() == 2 * 2**0.5
    
def test_locating():
    l1 = l.LineString([(0,0), (1,1), (2,2)])
    assert l1.locate_point(l.Point(1,1)) == 0.5

    l1 = l.LineString([(0,0), (1,1), (2,0)])
    m = l1.locate_point(l.Point(1,3))
    print m
    assert m == 0.5
    m = l1.locate_point(l.Point(0.5,0.5))
    print m
    assert m == 0.25
    
def test_substring():
    l1 = l.LineString([(0,0), (1,1), (2,2)])
    l2 = l1.substring(0, 0.5)
    print l2
    assert l2.length() == 2**0.5
    assert len(l2) == 2
    l2 = l1.substring(0, 0.75)
    assert len(l2) == 3
    assert l2.length() == 2**0.5 + (2*(0.5**2))**0.5
    assert l2.pts[-1].x == 1.5
    assert l2.pts[-1].y == 1.5

if __name__ == '__main__':
    test_basic()
    test_linestring()