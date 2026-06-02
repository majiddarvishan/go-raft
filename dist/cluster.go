package dist

type Cluster struct {
	node *Node
}

func NewCluster(cfg Config) *Cluster {
	return &Cluster{
		node: New(cfg),
	}
}

func (c *Cluster) Start() error {
	return c.node.Start()
}

func (c *Cluster) Put(k string, v []byte) {
	c.node.state.Put(k, v)
}

func (c *Cluster) Get(k string) []byte {
	return c.node.state.Get(k)
}