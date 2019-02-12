import json
from datetime import datetime

from toll_booth.alg_obj import AlgObject
from toll_booth.alg_obj.graph import InternalId
from toll_booth.alg_obj.graph.ogm.regulators import IdentifierStem, PotentialVertex, PotentialEdge
from toll_booth.alg_obj.serializers import AlgEncoder, AlgDecoder


class LinkEntry(AlgObject):
    def __init__(self, link_utc_timestamp: datetime, is_unlink=False):
        self._link_utc_timestamp = link_utc_timestamp
        self._is_unlink = is_unlink

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['link_utc_timestamp'], json_dict.get('is_unlink', False))

    @classmethod
    def parse_from_history_value(cls, history_value):
        link_utc_str, link_type = history_value.split('!')
        is_unlink = link_type == 'unlink'
        link_utc_timestamp = datetime.utcfromtimestamp(float(link_utc_str))
        return cls(link_utc_timestamp, is_unlink)

    @property
    def link_utc_timestamp(self):
        return self._link_utc_timestamp

    @property
    def is_unlink(self):
        return self._is_unlink

    @property
    def for_index(self):
        link_type = 'link'
        if self._is_unlink:
            link_type = 'unlink'
        return f'{self._link_utc_timestamp.timestamp()}!{link_type}'


class LinkHistory(AlgObject):
    def __init__(self, potential_vertex: PotentialVertex, link_entries=None):
        if not link_entries:
            link_entries = set()
        self._potential_vertex = potential_vertex
        self._id_value = potential_vertex.id_value
        self._link_entries = link_entries

    @classmethod
    def parse_json(cls, json_dict):
        return cls(json_dict['potential_vertex'], json_dict.get('link_entries'))

    @classmethod
    def parse_from_table_entry(cls, table_entry):
        potential_vertex = json.loads(table_entry['object_value'], cls=AlgDecoder)
        link_entries = {LinkEntry.parse_from_history_value(x) for x in table_entry['link_entries']}
        return cls(potential_vertex, link_entries)

    @classmethod
    def for_first_link(cls, potential_vertex, link_utc_timestamp):
        link_entries = [LinkEntry(link_utc_timestamp)]
        return cls(potential_vertex, link_entries)

    @property
    def potential_vertex(self):
        return self._potential_vertex

    @property
    def id_value(self):
        return self._id_value

    @property
    def most_recent_link(self):
        links = sorted([x for x in self._link_entries], key=lambda x: x.link_utc_timestamp, reverse=True)
        for link in links:
            return link

    @property
    def currently_linked(self):
        return not self.most_recent_link.is_unlink

    @property
    def internal_id(self):
        from_internal_id = self._potential_vertex.internal_id
        id_string = f'fip_link{from_internal_id}'
        return InternalId(id_string)

    @property
    def identifier_stem(self):
        paired_identifiers = self._potential_vertex.identifier_stem.paired_identifiers
        link_identifier_stem = IdentifierStem('edge', '_fip_link_', paired_identifiers)
        return link_identifier_stem

    @property
    def for_index(self):
        indexed = {
            'identifier_stem': str(self.identifier_stem),
            'sid_value': str(self._id_value),
            'id_value': self._id_value,
            'internal_id': str(self.internal_id),
            'object_type': '_fip_link_',
            'link_entries': {x.for_index for x in self._link_entries},
            'object_value': json.dumps(self._potential_vertex, cls=AlgEncoder)
        }
        return indexed

    @property
    def is_identifiable(self):
        return True

    @property
    def object_properties(self):
        return self.for_index

    def add_link_entry(self, link_entry):
        self._link_entries.add(link_entry)

    def generate_edge(self, link_entry):
        internal_id_values = ''.join([self.internal_id.id_value, str(link_entry.link_utc_timestamp.timestamp())])
        edge_internal_id = InternalId(internal_id_values)
        edge_properties = {
            'is_unlink': link_entry.is_unlink,
            'link_time': link_entry.link_utc_timestamp
        }
        from_id = self._potential_vertex.internal_id
        to_id = InternalId(''.join(['IdSource', self._potential_vertex['id_source']])).id_value
        edge = PotentialEdge('edge', edge_internal_id.id_value, edge_properties, from_id, to_id)
        return edge
