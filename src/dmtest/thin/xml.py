import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Dict, Union
import uuid

@dataclass
class Mapping:
    origin_begin: int
    data_begin: int
    length: int
    time: int
    is_single: bool = False

@dataclass
class Thin:
    dev_id: int
    mapped_blocks: int = 0
    transaction: int = 0
    creation_time: int = 0
    snap_time: int = 0
    mappings: List[Mapping] = field(default_factory=list)

    def add_mapping(self, origin_begin: int, data_begin: int, length: int, time: int):
        self.mappings.append(Mapping(origin_begin, data_begin, length, time, length == 1))
        self.mapped_blocks += length

    def add_single_mapping(self, origin_block: int, data_block: int, time: int):
        self.mappings.append(Mapping(origin_block, data_block, 1, time, True))
        self.mapped_blocks += 1

@dataclass
class Pool:
    data_block_size: int
    nr_data_blocks: int
    transaction: int = 0
    time: int = 0
    uuid: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: int = 2
    thins: Dict[int, Thin] = field(default_factory=dict)

    def add_thin(self, dev_id: int) -> Thin:
        thin = Thin(dev_id)
        self.thins[dev_id] = thin
        return thin

def num(n):
    return str(int(n))

def generate_xml(pool: Pool) -> ET.Element:
    root = ET.Element("superblock")
    root.set("uuid", pool.uuid)
    root.set("time", num(pool.time))
    root.set("transaction", num(pool.transaction))
    root.set("version", num(pool.version))
    root.set("data_block_size", num(pool.data_block_size))
    root.set("nr_data_blocks", num(pool.nr_data_blocks))

    for thin in pool.thins.values():
        device = ET.SubElement(root, "device")
        device.set("dev_id", num(thin.dev_id))
        device.set("mapped_blocks", num(thin.mapped_blocks))
        device.set("transaction", num(thin.transaction))
        device.set("creation_time", num(thin.creation_time))
        device.set("snap_time", num(thin.snap_time))

        for mapping in thin.mappings:
            if mapping.is_single:
                m = ET.SubElement(device, "single_mapping")
                m.set("origin_block", num(mapping.origin_begin))
                m.set("data_block", num(mapping.data_begin))
                m.set("time", num(mapping.time))
            else:
                m = ET.SubElement(device, "range_mapping")
                m.set("origin_begin", num(mapping.origin_begin))
                m.set("data_begin", num(mapping.data_begin))
                m.set("length", num(mapping.length))
                m.set("time", num(mapping.time))

    return root

def write_xml(pool: Pool, file_path: str):
    root = generate_xml(pool)
    tree = ET.ElementTree(root)
    tree.write(file_path, encoding="utf-8", xml_declaration=True)

