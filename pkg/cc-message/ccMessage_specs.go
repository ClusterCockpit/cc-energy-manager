package ccmessage

import (
	"fmt"
	"strconv"
)

const CC_TYPE_TAG = "type"
const CC_TYPEID_TAG = "type-id"

type ValidTypeTags int

const (
	TYPE_TAG_NODE ValidTypeTags = iota
	TYPE_TAG_SOCKET
	TYPE_TAG_DIE
	TYPE_TAG_MEM_DOMAIN
	TYPE_TAG_LLC
	TYPE_TAG_CORE
	TYPE_TAG_HWTHREAD
	TYPE_TAG_JOBID
	TYPE_TAG_ACCELERATOR
)
const MIN_TYPE_TAG = TYPE_TAG_NODE
const MAX_TYPE_TAG = TYPE_TAG_JOBID
const TYPE_TAG_ANY = MAX_TYPE_TAG + 1
const TYPE_TAG_UNKNOWN = MAX_TYPE_TAG + 2

func (t *ValidTypeTags) String() string {
	switch *t {
	case TYPE_TAG_NODE:
		return "node"
	case TYPE_TAG_ACCELERATOR:
		return "accelerator"
	case TYPE_TAG_CORE:
		return "core"
	case TYPE_TAG_DIE:
		return "die"
	case TYPE_TAG_JOBID:
		return "jobid"
	case TYPE_TAG_LLC:
		return "llc"
	case TYPE_TAG_MEM_DOMAIN:
		return "memoryDomain"
	case TYPE_TAG_SOCKET:
		return "socket"
	case TYPE_TAG_HWTHREAD:
		return "hwthread"
	case TYPE_TAG_UNKNOWN:
		return "unknown"
	}
	return "unknown"
}

func ParseTypeTag(tag string) ValidTypeTags {
	for i := MIN_TYPE_TAG; i <= MAX_TYPE_TAG; i++ {
		if tag == i.String() {
			return i
		}
	}
	return TYPE_TAG_UNKNOWN
}

func (m *ccMessage) Validate(hostnameTag string) bool {
	if _, ok := m.GetTag(hostnameTag); !ok {
		return false
	}
	if t, ok := m.GetTag(CC_TYPE_TAG); ok {
		typetag := ParseTypeTag(t)
		if typetag < MIN_TYPE_TAG || typetag > MAX_TYPE_TAG {
			return false
		}
		mtype := m.MessageType()
		if mtype < MIN_CCMSG_TYPE || mtype > MAX_CCMSG_TYPE {
			return false
		}
		zero_typeid := false
		pci_typeid := false
		switch typetag {
		case TYPE_TAG_NODE:
			zero_typeid = true
		case TYPE_TAG_ACCELERATOR:
			pci_typeid = true
		}
		if !zero_typeid {
			if typeidtag, ok := m.GetTag(CC_TYPEID_TAG); ok {
				if !pci_typeid {
					i_tid, err := strconv.ParseInt(typeidtag, 10, 64)
					if err != nil || i_tid < 0 {
						return false
					}
				} else {
					var dom int = 0
					var bus int = 0
					var dev int = 0
					var f int = 0
					c, err := fmt.Sscanf(typeidtag, "%08X:%02X:%02X.%X", &dom, &bus, &dev, &f)
					if err != nil || c != 3 {
						return false
					}
				}
			} else {
				return false
			}
		} else {
			if typeidtag, ok := m.GetTag(CC_TYPEID_TAG); ok {
				if typeidtag != "0" {
					return false
				}
			}
		}
		return true
	}
	return false
}
