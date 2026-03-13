"""Graph builder: BFS transitive closure from DirectEdges to Relationships."""

from __future__ import annotations

from collections import deque

from dbt_linter.models import DirectEdge, Relationship, Resource


def build_relationships(
    resources: list[Resource],
    edges: list[DirectEdge],
) -> list[Relationship]:
    """Build transitive closure of all resource relationships via BFS.

    For each resource, performs BFS over the adjacency list to discover all
    reachable descendants. Each (parent, descendant) pair produces a
    Relationship with shortest-path distance and chain-of-views flag.

    Edges referencing resource IDs not present in *resources* are skipped.
    """
    if not resources or not edges:
        return []

    lookup: dict[str, Resource] = {r.resource_id: r for r in resources}
    children = _build_adjacency_list(edges, lookup)
    relationships: list[Relationship] = []

    for source_id, source in lookup.items():
        if source_id not in children:
            continue

        # visited maps node_id -> (distance, parent_in_bfs_tree)
        visited: dict[str, tuple[int, str | None]] = {source_id: (0, None)}
        queue: deque[str] = deque([source_id])

        while queue:
            current = queue.popleft()
            current_dist = visited[current][0]

            for child_id in children.get(current, []):
                if child_id not in visited:
                    visited[child_id] = (current_dist + 1, current)
                    queue.append(child_id)

        relationships.extend(
            _relationships_from_bfs(visited, source_id, source, lookup)
        )

    return relationships


def _build_adjacency_list(
    edges: list[DirectEdge],
    lookup: dict[str, Resource],
) -> dict[str, list[str]]:
    """Build parent->children adjacency list, filtering edges with missing resources."""
    children: dict[str, list[str]] = {}
    for edge in edges:
        if edge.parent in lookup and edge.child in lookup:
            children.setdefault(edge.parent, []).append(edge.child)
    return children


def _relationships_from_bfs(
    visited: dict[str, tuple[int, str | None]],
    source_id: str,
    source: Resource,
    lookup: dict[str, Resource],
) -> list[Relationship]:
    """Convert BFS visited map into Relationship objects for all descendants."""
    relationships: list[Relationship] = []
    for dest_id, (dist, _) in visited.items():
        if dest_id == source_id:
            continue

        dest = lookup[dest_id]
        chain_of_views = _is_chain_of_views(visited, lookup, source_id, dest_id)

        relationships.append(
            Relationship(
                parent=source_id,
                child=dest_id,
                parent_resource_type=source.resource_type,
                child_resource_type=dest.resource_type,
                parent_model_type=source.model_type,
                child_model_type=dest.model_type,
                parent_materialization=source.materialization,
                parent_is_public=source.is_public,
                distance=dist,
                is_dependent_on_chain_of_views=chain_of_views,
            )
        )
    return relationships


def _is_chain_of_views(
    visited: dict[str, tuple[int, str | None]],
    lookup: dict[str, Resource],
    source_id: str,
    dest_id: str,
) -> bool:
    """Check if every intermediate node on the shortest path is a view.

    Intermediate nodes exclude source and destination. For distance=1,
    there are no intermediates, so the result is always False.
    """
    dist = visited[dest_id][0]
    if dist <= 1:
        return False

    # Walk backwards from dest to source via BFS parent pointers.
    current = dest_id
    while True:
        parent_id = visited[current][1]
        if parent_id is None or parent_id == source_id:
            break
        if lookup[parent_id].materialization != "view":
            return False
        current = parent_id

    return True
