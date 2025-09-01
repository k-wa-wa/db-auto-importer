package graph

import (
	"fmt"
	"log"
	"sort"

	"db-auto-importer/internal/database"
)

// Graph represents the dependency graph of tables.
type Graph struct {
	Nodes map[string]*Node
}

// Node represents a table in the dependency graph.
type Node struct {
	TableName string
	Edges     []*Node // Tables that depend on this table (children)
	InDegree  int     // Number of tables this table depends on (parents)
}

// NewGraph creates a new Graph instance from database schema information.
func NewGraph(schemaInfo map[string]database.DBInfo) *Graph {
	nodes := make(map[string]*Node)
	for tableName := range schemaInfo {
		nodes[tableName] = &Node{TableName: tableName}
	}

	for _, dbInfo := range schemaInfo {
		for _, fk := range dbInfo.ForeignKeys {
			// fk.TableName (child) depends on fk.ForeignTableName (parent)
			childNode := nodes[fk.TableName]
			parentNode := nodes[fk.ForeignTableName]

			if childNode == nil || parentNode == nil {
				log.Printf("Warning: Foreign key references non-existent table. Child: %s, Parent: %s\n", fk.TableName, fk.ForeignTableName)
				continue
			}

			// Add parentNode to childNode's edges (child depends on parent)
			// This is actually reversed for topological sort.
			// For topological sort, we need edges from parent to child.
			// So, parentNode has an edge to childNode.
			parentNode.Edges = append(parentNode.Edges, childNode)
			childNode.InDegree++
		}
	}
	return &Graph{Nodes: nodes}
}

// TopologicalSort performs a topological sort on the graph to determine import order.
func (g *Graph) TopologicalSort() ([]string, error) {
	var order []string
	queue := []string{} // Queue for nodes with in-degree 0

	// Create a temporary map for in-degrees to preserve the original graph structure
	currentInDegrees := make(map[string]int)
	for tableName, node := range g.Nodes {
		currentInDegrees[tableName] = node.InDegree
	}

	// Collect all nodes with an in-degree of 0
	var initialZeroInDegreeNodes []string
	for tableName, inDegree := range currentInDegrees {
		if inDegree == 0 {
			initialZeroInDegreeNodes = append(initialZeroInDegreeNodes, tableName)
		}
	}

	// Sort them to ensure deterministic order
	sort.Strings(initialZeroInDegreeNodes)
	queue = append(queue, initialZeroInDegreeNodes...)

	for len(queue) > 0 {
		// Dequeue a node
		tableName := queue[0]
		queue = queue[1:]
		order = append(order, tableName)

		// For each neighbor of the dequeued node
		for _, neighbor := range g.Nodes[tableName].Edges {
			currentInDegrees[neighbor.TableName]--
			// If neighbor's in-degree becomes 0, enqueue it
			if currentInDegrees[neighbor.TableName] == 0 {
				queue = append(queue, neighbor.TableName)
			}
		}
	}

	// Check for cycles
	if len(order) != len(g.Nodes) {
		return nil, fmt.Errorf("cycle detected in table dependencies. Cannot determine a valid import order.")
	}

	return order, nil
}
