package elasticsearch

var mapping = `{
	"mappings": {
	    "_default_": {
		"dynamic_templates": [
		    {
			"strings": {
			    "mapping": {
				"index": "not_analyzed",
				"type": "string"
			    },
			    "match_mapping_type": "string"
			}
		    }
		],
		"_all": {
		    "enabled": false
		},
		"properties": {}
	    },
	    "metric_index": {
		"dynamic_templates": [
		    {
			"strings": {
			    "mapping": {
				"index": "not_analyzed",
				"type": "string"
			    },
			    "match_mapping_type": "string"
			}
		    }
		],
		"_all": {
		    "enabled": false
		},
		"_timestamp": {
		    "enabled": false
		},
		"properties": {
		    "id": {
			"type": "string",
			"index": "not_analyzed"
		    },
		    "interval": {
			"type": "long"
		    },
		    "lastUpdate": {
			"type": "long"
		    },
		    "metric": {
			"type": "string",
			"index": "not_analyzed"
		    },
		    "name": {
			"type": "string",
			"index": "not_analyzed"
		    },
		    "node_count": {
			"type": "long"
		    },
		    "org_id": {
			"type": "long"
		    },
		    "tags": {
			"type": "string",
			"index": "not_analyzed"
		    },
		    "mtype": {
			"type": "string",
			"index": "not_analyzed"
		    },
		    "unit": {
			"type": "string",
			"index": "not_analyzed"
		    }
		}
            }
	}
}`
