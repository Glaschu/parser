#!/usr/bin/env python3
"""
OpenLineage Event Generator
===========================

Converts C# SQL parser metadata into OpenLineage events with column lineage facets.
Generates events for the banking settlement procedure showing data transformations.

Features:
- Creates START/RUNNING/COMPLETE events
- Generates column lineage facets with transformations
- Maps temp tables and CTEs to proper namespaces
- Handles direct and indirect transformations
- Follows OpenLineage 1.2.0 specification
"""

import json
import uuid
import sys
from datetime import datetime, timezone
from typing import Dict, List, Set, Optional
from collections import defaultdict


class OpenLineageGenerator:
    def __init__(self, metadata_file: str, lineage_file: str, schema_file: str):
        """Initialize with C# metadata, lineage analysis, and schema."""
        print("🔍 Initializing OpenLineage Event Generator")
        print("=" * 50)
        
        try:
            with open(metadata_file, 'r') as f:
                self.metadata = json.load(f)
            print(f"✅ Loaded C# metadata from {metadata_file}")
            
            with open(lineage_file, 'r') as f:
                self.lineage_data = json.load(f)
            print(f"✅ Loaded lineage analysis from {lineage_file}")
            
            with open(schema_file, 'r') as f:
                self.schema = json.load(f)
            print(f"✅ Loaded schema from {schema_file}")
        except Exception as e:
            print(f"❌ Error loading files: {e}")
            sys.exit(1)
        
        # OpenLineage configuration
        self.namespace = "banking_settlement"
        self.producer = "https://github.com/Glaschu/parser/sql-lineage-parser/1.0"
        self.schema_url = "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json"
        
        # Generate run and job info
        self.run_id = str(uuid.uuid4())
        self.job_name = self.metadata.get('procedure_name', 'usp_ProcessDailyCoreBankingSettlement_Monster')
        
        print(f"🏃 Run ID: {self.run_id}")
        print(f"📋 Job: {self.job_name}")
        print(f"📊 Schema tables: {len(self.schema)} tables")
        print("")

    def _normalize_table_name(self, table_name: str) -> str:
        """Normalize table names for OpenLineage."""
        # Remove # prefix from temp tables
        if table_name.startswith('#'):
            return f"temp.{table_name[1:]}"
        
        # Handle CTE aliases
        temp_indicators = ['x', 'j', 'a', 'r', 'scores', 'feerule', 'feecalc', 'bal',
                          'needcheck', 'slice', 'map', 'src', 'joinmap', 'net']
        
        if table_name.lower() in [t.lower() for t in temp_indicators]:
            return f"cte.{table_name}"
        
        # Standard schema.table format
        if '.' not in table_name:
            return f"public.{table_name}"
        
        return table_name

    def _get_transformation_type(self, lineage: Dict) -> Dict:
        """Determine transformation type based on lineage path."""
        path_length = lineage.get('path_length', 1)
        source_table = lineage.get('source_table', '')
        target_table = lineage.get('target_table', '')
        
        # Direct mapping (1 step)
        if path_length == 1:
            if source_table == target_table:
                return {
                    "type": "DIRECT",
                    "subtype": "IDENTITY",
                    "description": "Direct column copy",
                    "masking": False
                }
            else:
                return {
                    "type": "DIRECT",
                    "subtype": "TRANSFORMATION",
                    "description": "Direct column transformation",
                    "masking": False
                }
        
        # Multi-step transformation
        else:
            return {
                "type": "INDIRECT",
                "subtype": "TRANSFORMATION",
                "description": f"Multi-step transformation ({path_length} steps)",
                "masking": False
            }

    def _build_column_lineage_facet(self, target_table: str) -> Dict:
        """Build column lineage facet for a target table."""
        facet = {
            "_producer": self.producer,
            "_schemaURL": self.schema_url,
            "fields": {},
            "dataset": []
        }
        
        # Get all lineages for this target table
        target_lineages = []
        
        for lineage in self.lineage_data.get('direct_lineages', []):
            target = lineage.get('target', '')
            if '.' in target:
                table_name = target.rsplit('.', 1)[0]
                if table_name == target_table:
                    target_lineages.append(lineage)
        
        for lineage in self.lineage_data.get('indirect_lineages', []):
            target = lineage.get('target', '')
            if '.' in target:
                table_name = target.rsplit('.', 1)[0]
                if table_name == target_table:
                    target_lineages.append(lineage)
        
        # Group by target column
        columns_map = defaultdict(list)
        for lineage in target_lineages:
            target = lineage.get('target', '')
            if '.' in target:
                target_col = target.rsplit('.', 1)[1]
                columns_map[target_col].append(lineage)
        
        # Build field lineages
        for target_col, lineages in columns_map.items():
            input_fields = []
            
            for lineage in lineages:
                source = lineage.get('source', '')
                if '.' in source:
                    source_table = source.rsplit('.', 1)[0]
                    source_column = source.rsplit('.', 1)[1]
                    
                    source_table_norm = self._normalize_table_name(source_table)
                    transformation = self._get_transformation_type(lineage)
                    
                    input_field = {
                        "namespace": self.namespace,
                        "name": source_table_norm,
                        "field": source_column,
                        "transformations": [transformation]
                    }
                    input_fields.append(input_field)
            
            facet["fields"][target_col] = {
                "inputFields": input_fields
            }
        
        # Build dataset-level transformations (for sorting, grouping, etc.)
        dataset_transformations = set()
        for lineage in target_lineages:
            if lineage.get('path_length', 1) > 1:
                source = lineage.get('source', '')
                if '.' in source:
                    source_table = source.rsplit('.', 1)[0]
                    source_column = source.rsplit('.', 1)[1]
                    
                    source_table_norm = self._normalize_table_name(source_table)
                    
                    dataset_entry = {
                        "namespace": self.namespace,
                        "name": source_table_norm,
                        "field": source_column,
                        "transformations": [{
                            "type": "INDIRECT",
                            "subtype": "AGGREGATION",
                            "description": "Part of complex transformation chain",
                            "masking": False
                        }]
                    }
                    
                    # Convert to tuple for set deduplication
                    dataset_key = (dataset_entry["name"], dataset_entry["field"])
                    if dataset_key not in {(d.get("name"), d.get("field")) for d in facet["dataset"]}:
                        facet["dataset"].append(dataset_entry)
        
        return facet

    def _get_input_datasets(self) -> List[Dict]:
        """Get all input datasets from schema (staging and ref tables)."""
        inputs = []
        
        for table_name in self.schema.keys():
            if table_name.startswith('staging.') or table_name.startswith('ref.'):
                inputs.append({
                    "namespace": self.namespace,
                    "name": self._normalize_table_name(table_name),
                    "facets": {
                        "schema": {
                            "_producer": self.producer,
                            "_schemaURL": "https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json",
                            "fields": [
                                {
                                    "name": col_name,
                                    "type": col_type,
                                    "description": f"Column from {table_name}"
                                }
                                for col_name, col_type in self.schema[table_name].items()
                            ]
                        }
                    }
                })
        
        return sorted(inputs, key=lambda x: x["name"])

    def _get_output_datasets(self) -> List[Dict]:
        """Get all output datasets from schema (core, audit, ops tables) with column lineage facets."""
        outputs = []
        
        for table_name in self.schema.keys():
            if (table_name.startswith('core.') or 
                table_name.startswith('audit.') or 
                table_name.startswith('ops.')):
                
                # Build column lineage facet for this table
                column_lineage_facet = self._build_column_lineage_facet(table_name)
                
                output = {
                    "namespace": self.namespace,
                    "name": self._normalize_table_name(table_name),
                    "facets": {
                        "schema": {
                            "_producer": self.producer,
                            "_schemaURL": "https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json",
                            "fields": [
                                {
                                    "name": col_name,
                                    "type": col_type,
                                    "description": f"Column in {table_name}"
                                }
                                for col_name, col_type in self.schema[table_name].items()
                            ]
                        },
                        "columnLineage": column_lineage_facet
                    }
                }
                outputs.append(output)
        
        return sorted(outputs, key=lambda x: x["name"])

    def generate_event(self, event_type: str = "COMPLETE") -> Dict:
        """Generate a complete OpenLineage event."""
        timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        
        event = {
            "eventType": event_type,
            "eventTime": timestamp,
            "run": {
                "runId": self.run_id,
                "facets": {
                    "nominalTime": {
                        "_producer": self.producer,
                        "_schemaURL": "https://openlineage.io/spec/facets/1-2-0/NominalTimeRunFacet.json",
                        "nominalStartTime": timestamp
                    }
                }
            },
            "job": {
                "namespace": self.namespace,
                "name": self.job_name,
                "facets": {
                    "sql": {
                        "_producer": self.producer,
                        "_schemaURL": "https://openlineage.io/spec/facets/1-2-0/SqlJobFacet.json",
                        "query": f"-- {self.job_name}\n-- Banking settlement procedure with complex transformations\n-- Processed at {timestamp}"
                    }
                }
            },
            "inputs": self._get_input_datasets(),
            "outputs": self._get_output_datasets(),
            "producer": self.producer
        }
        
        return event

    def generate_all_events(self) -> List[Dict]:
        """Generate START, RUNNING, and COMPLETE events."""
        events = []
        
        from datetime import timedelta
        base_timestamp = datetime.now(timezone.utc)
        
        # START event
        start_event = self.generate_event("START")
        start_event["eventTime"] = base_timestamp.isoformat().replace('+00:00', 'Z')
        start_event["outputs"] = []  # No outputs yet
        events.append(start_event)
        
        # RUNNING event (optional)
        running_event = self.generate_event("RUNNING")
        running_timestamp = base_timestamp + timedelta(seconds=1)
        running_event["eventTime"] = running_timestamp.isoformat().replace('+00:00', 'Z')
        running_event["outputs"] = []  # No outputs yet
        events.append(running_event)
        
        # COMPLETE event
        complete_event = self.generate_event("COMPLETE")
        complete_timestamp = base_timestamp + timedelta(seconds=2)
        complete_event["eventTime"] = complete_timestamp.isoformat().replace('+00:00', 'Z')
        events.append(complete_event)
        
        return events

    def save_events(self, events: List[Dict], output_file: str):
        """Save events to JSON file."""
        output_data = {
            "openlineage_version": "1.2.0",
            "generated_at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "generator": "SQL Lineage Parser - C# Edition",
            "procedure": self.job_name,
            "events": events,
            "summary": {
                "total_events": len(events),
                "input_datasets": len(events[-1].get("inputs", [])),
                "output_datasets": len(events[-1].get("outputs", [])),
                "total_lineages": self.lineage_data.get("statistics", {}).get("total_lineages", 0)
            }
        }
        
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"💾 OpenLineage events saved to {output_file}")

    def display_summary(self, events: List[Dict]):
        """Display a summary of generated events."""
        print("\n" + "=" * 70)
        print("🎯 OPENLINEAGE EVENT GENERATION SUMMARY")
        print("=" * 70)
        
        complete_event = events[-1]  # Last event should be COMPLETE
        
        print(f"📋 Job: {self.job_name}")
        print(f"🏃 Run ID: {self.run_id}")
        print(f"📅 Generated: {len(events)} events")
        print("")
        
        print(f"📥 Input Datasets: {len(complete_event.get('inputs', []))}")
        for input_ds in complete_event.get('inputs', []):
            print(f"   • {input_ds['name']}")
        
        print(f"\n📤 Output Datasets: {len(complete_event.get('outputs', []))}")
        for output_ds in complete_event.get('outputs', []):
            facets = output_ds.get('facets', {})
            column_lineage = facets.get('columnLineage', {})
            field_count = len(column_lineage.get('fields', {}))
            print(f"   • {output_ds['name']} ({field_count} columns with lineage)")
        
        print(f"\n🔗 Total Column Lineages: {self.lineage_data.get('statistics', {}).get('total_lineages', 0)}")
        print("=" * 70)


def main():
    """Main execution function."""
    print("🚀 OpenLineage Event Generator")
    print("=" * 40)
    print("Converting C# metadata to OpenLineage events...")
    print("")
    
    # File paths
    metadata_file = "/Users/jamesglasgow/Projects/parser/csharp_metadata.json"
    lineage_file = "/Users/jamesglasgow/Projects/parser/end_to_end_lineages_analysis.json"
    schema_file = "/Users/jamesglasgow/Projects/parser/schema.json"
    
    # Initialize generator
    generator = OpenLineageGenerator(metadata_file, lineage_file, schema_file)
    
    # Generate events
    print("🔄 Generating OpenLineage events...")
    events = generator.generate_all_events()
    
    # Display summary
    generator.display_summary(events)
    
    # Save events
    output_file = "/Users/jamesglasgow/Projects/parser/openlineage_events.json"
    generator.save_events(events, output_file)
    
    print(f"\n🎉 Successfully generated {len(events)} OpenLineage events!")
    print("Events are compatible with OpenLineage 1.2.0 specification")


if __name__ == "__main__":
    main()
