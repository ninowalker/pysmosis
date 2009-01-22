--
-- Database: openstreetmap
--

-- --------------------------------------------------------
--
-- Table structure for table nodes
--

CREATE TABLE nodes (
  id INTEGER PRIMARY KEY,
  latitude REAL NOT NULL,
  longitude REAL NOT NULL,
  user_id TEXT,
  visible INTEGER NOT NULL,
  "timestamp" TEXT NOT NULL
);
CREATE INDEX nodes_timestamp_idx ON nodes ("timestamp");
CREATE INDEX nodes_latlng_idx ON nodes (latitude, longitude);

-- --------------------------------------------------------

--
-- Table structure for table node_tags
--

CREATE TABLE node_tags (
  id INTEGER NOT NULL,
  k TEXT NOT NULL,
  v TEXT NOT NULL
);
CREATE INDEX node_tags_id_idx ON node_tags(id);
CREATE INDEX node_tags_k_idx ON node_tags(k);


-- Table for node refs, which count the number of references to a particular node.
CREATE TABLE node_refs (
	id INTEGER PRIMARY KEY,
	cnt INTEGER DEFAULT 0
);

-- --------------------------------------------------------

--
-- Table structure for table relations
--

CREATE TABLE relations (
  id INTEGER PRIMARY KEY,
  user_id TEXT NOT NULL,
  "timestamp" TEXT NOT NULL,
  visible INTEGER NOT NULL default 1
);
CREATE INDEX relations_timestamp_idx ON relations ("timestamp");
-- --------------------------------------------------------

--
-- Table structure for table relation_members
--

CREATE TABLE relation_members (
  id INTEGER NOT NULL,
  member_type TEXT NOT NULL,
  member_id INTEGER NOT NULL,
  member_role TEXT NOT NULL,
  PRIMARY KEY  (id,member_type,member_id,member_role)
);
CREATE INDEX relation_members_member_idx ON relation_members (member_type,member_id);
-- --------------------------------------------------------

--
-- Table structure for table relation_tags
--

CREATE TABLE relation_tags (
  id INTEGER NOT NULL,
  k TEXT NOT NULL,
  v TEXT NOT NULL
);
CREATE INDEX relation_tags_id_idx ON relation_tags(id);
CREATE INDEX relation_tags_k_idx ON relation_tags(k);
-- --------------------------------------------------------

--
-- Table structure for table ways
--

CREATE TABLE ways (
  id INTEGER PRIMARY KEY,
  user_id TEXT,
  "timestamp" TEXT NOT NULL,
  visible INTEGER NOT NULL default 1
);
CREATE INDEX ways_timestamp_idx ON ways ("timestamp");
-- --------------------------------------------------------

--
-- Table structure for table way_nodes
--

CREATE TABLE way_nodes (
  id INTEGER NOT NULL,
  node_id INTEGER NOT NULL,
  sequence_id INTEGER NOT NULL,
  PRIMARY KEY  (id,sequence_id)
  
);
CREATE INDEX way_nodes_node_idx ON way_nodes (node_id);

-- --------------------------------------------------------

--
-- Table structure for table way_tags
--

CREATE TABLE way_tags (
  id INTEGER NOT NULL,
  k TEXT NOT NULL,
  v TEXT NOT NULL
);
CREATE INDEX way_tags_id_idx ON way_tags(id);
CREATE INDEX way_tags_k_idx ON way_tags(k);


CREATE TABLE way_segments (
  id INTEGER NOT NULL,
  sequence_id INTEGER NOT NULL,
  start_node_id INTEGER NOT NULL,
  end_node_id INTEGER NOT NULL,
  wkt TEXT NOT NULL,
  PRIMARY KEY  (id,sequence_id)
);
CREATE INDEX way_segments_start_node_idx ON way_segments(start_node_id);
CREATE INDEX way_segments_end_node_idx ON way_segments(end_node_id);

--
-- Table structure for bounds, as found in an xml import
--

CREATE TABLE bounds (
	--id INTEGER PRIMARY KEY,
	minlat REAL NOT NULL,
	minlng REAL NOT NULL,
	maxlat REAL NOT NULL,
	maxlng REAL NOT NULL
);
