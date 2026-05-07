# Rules reference

Use `dbt-lint --list-rules` for full details including rationale and remediation guidance.

## Modeling

| Rule | Description |
| --- | --- |
| `modeling/direct-join-to-source` | Models referencing both source and model parents |
| `modeling/downstream-depends-on-source` | Intermediate/marts models depending directly on sources |
| `modeling/staging-depends-on-staging` | Staging models depending on other staging models |
| `modeling/staging-depends-on-downstream` | Staging models depending on intermediate/marts |
| `modeling/mart-depends-on-mart` | Mart models depending on other mart models |
| `modeling/root-models` | Models with zero parents |
| `modeling/hard-coded-references` | Hard-coded table references in SQL |
| `modeling/duplicate-sources` | Multiple source entries for the same table |
| `modeling/unused-sources` | Sources with no downstream consumers |
| `modeling/multiple-sources-joined` | Models joining more than one source directly |
| `modeling/source-fanout` | Sources with multiple direct child models |
| `modeling/model-fanout` | Models exceeding the fanout threshold |
| `modeling/too-many-joins` | Models with too many direct parents |
| `modeling/staging-model-too-many-parents` | Staging models with more than one parent |
| `modeling/intermediate-fanout` | Intermediate models with too many direct dependents |
| `modeling/rejoining-upstream-concepts` | Models that rejoin a previously consumed concept |
| `modeling/duplicate-mart-concepts` | Same entity modeled in multiple mart subdirectories |

## Testing

| Rule | Description |
| --- | --- |
| `testing/missing-primary-key-tests` | Models without primary key uniqueness/not-null tests |
| `testing/missing-relationship-tests` | Models with model parents but no relationship tests |
| `testing/untested-models` | Models with no generic tests |
| `testing/sources-without-freshness` | Sources without freshness checks |
| `testing/test-coverage` | Test coverage below target by model type |

## Documentation

| Rule | Description |
| --- | --- |
| `documentation/undocumented-models` | Models without a description |
| `documentation/undocumented-sources` | Sources without a source-level description |
| `documentation/undocumented-source-tables` | Source tables without a table-level description |
| `documentation/documentation-coverage` | Documentation coverage below target by model type |
| `documentation/column-documentation-coverage` | Column documentation coverage below target (disabled by default) |

## Structure

| Rule | Description |
| --- | --- |
| `structure/model-name-format` | Model name is not valid snake_case |
| `structure/model-naming-conventions` | Model name doesn't match prefix for its type |
| `structure/column-naming-conventions` | Column name violates naming conventions (disabled by default) |
| `structure/model-directories` | Model not in expected directory for its type |
| `structure/source-directories` | Source YAML not in staging directory |
| `structure/test-directories` | Test YAML in different directory than model |
| `structure/staging-naming-convention` | Staging model doesn't follow `stg_<source>__<entity>` pattern |
| `structure/yaml-file-naming` | YAML schema file doesn't follow `_<dir>__<type>.yml` convention |
| `structure/staging-materialization` | Staging model with disallowed materialization |
| `structure/intermediate-materialization` | Intermediate model with disallowed materialization |
| `structure/marts-materialization` | Marts model with disallowed materialization |

## Performance

| Rule | Description |
| --- | --- |
| `performance/chained-views` | View chains exceeding depth threshold |
| `performance/exposure-parent-materializations` | Exposures with view/ephemeral/source parents |
| `performance/incremental-missing-unique-key` | Incremental model without unique_key config |

## Governance

| Rule | Description |
| --- | --- |
| `governance/public-models-without-contract` | Public models without contract enforcement |
| `governance/undocumented-public-models` | Public models missing description or column docs |
| `governance/intermediate-public-access` | Intermediate models with public access |
| `governance/exposures-depend-on-private-models` | Exposures depending on non-public models |
