#include "index_manager/bpt.h"

#include <algorithm>
#include <cstring>

#include "log.h"
#include "recovery.h"
#include "trx.h"

union bpt_leaf_page_t {
  page_t page;
  struct {
    bpt_header_t header;
    byte padding[112 - sizeof(header)];
    uint64_t free_space;
    pagenum_t right_sibling;
  } leaf_data;
};

struct leaf_slot_t {
  bpt_key_t key;
  uint16_t size;
  uint16_t offset;
  int32_t trx_id;
} __attribute__((packed));

union bpt_internal_page_t {
  page_t page;
  struct {
    bpt_header_t header;
    byte padding[120 - sizeof(header)];
    pagenum_t first_child_page;
  } internal_data;
};

struct internal_slot_t {
  bpt_key_t key;
  pagenum_t pagenum;
};

const uint64_t kMaxNumInternalPageEntries =
    (kPageSize - kBptPageHeaderSize) / sizeof(internal_slot_t);
const uint64_t kMergeOrDistributeThreshold = 2500;

// function definitions
void move_memory(byte *base, int64_t src_offset, int64_t delta, uint32_t size);

// get leaf slots array pointer
leaf_slot_t *leaf_slot_array(bpt_leaf_page_t *page);

// get internal slots array pointer
internal_slot_t *internal_slot_array(bpt_internal_page_t *page);

void init_leaf_page_struct(bpt_leaf_page_t *page, pagenum_t parent_page);

void init_internal_page_struct(bpt_internal_page_t *page,
                               pagenum_t parent_page);

// get neighbor pagenum
// if given page is first child then return right sibling
// else, return left sibling (0 on failed)
pagenum_t get_neighbor_pagenum(int64_t table_id, pagenum_t parent,
                               pagenum_t pagenum, bpt_key_t *key);

// change key of internal page
// return true on success
bool change_key(int64_t table_id, pagenum_t pagenum, bpt_key_t from,
                bpt_key_t to);

// set parent page
void set_parent_page(int64_t table_id, pagenum_t pagenum, pagenum_t parent);

// adjust root page after deletion
// return new root (0 on failed)
pagenum_t adjust_root(int64_t table_id, pagenum_t root);

// create new root node and insert keys
// return new root (0 on failed)
pagenum_t insert_into_new_root(int64_t table_id, pagenum_t left, bpt_key_t key,
                               pagenum_t right);

// insert new node into parent
// return root (0 on failed)
pagenum_t insert_into_parent(int64_t table_id, pagenum_t root, pagenum_t parent,
                             pagenum_t left, bpt_key_t key, pagenum_t right);

pagenum_t find_leaf(int64_t table_id, pagenum_t root, bpt_key_t key);

// insert new slot into bpt leaf page
// return true on success
bool insert_into_leaf(bpt_leaf_page_t *page, bpt_key_t key, uint16_t size,
                      const byte *value);

// insert new slot into bpt leaf page
// create new page and update sibling
// return root (0 on failed)
pagenum_t insert_into_leaf_after_splitting(int64_t table_id, pagenum_t root,
                                           pagenum_t pagenum,
                                           pagenum_t *sibling, bpt_key_t key,
                                           uint16_t size, const byte *value);

// delete entry from bpt leaf page
// return pagenum (0 on failed)
pagenum_t delete_entry_from_leaf(int64_t table_id, pagenum_t pagenum,
                                 bpt_key_t key);

// delete key from bpt leaf page
// handle merge, redistribute
// return root (0 on failed)
pagenum_t delete_from_leaf(int64_t table_id, pagenum_t root, pagenum_t pagenum,
                           bpt_key_t key);

// merget neighboring two leaf pages
// return root (0 on failed)
pagenum_t merge_leaf(int64_t table_id, pagenum_t root, bpt_key_t key_in_parent,
                     pagenum_t pagenum, pagenum_t neighbor_pagenum);

// redistribute leaf slots
// return root (0 on failed)
pagenum_t redistribute_leaf(int64_t table_id, pagenum_t root,
                            bpt_key_t key_in_parent, pagenum_t pagenum,
                            pagenum_t neighbor_pagenum);

// insert new slot into bpt internal page
// also set the parent page of the newly inserted page
// return true on success
bool insert_into_internal(int64_t table_id, pagenum_t pagenum,
                          bpt_internal_page_t *page, int left_idx,
                          bpt_key_t key, pagenum_t val);

// insert new slot into bpt internal page
// create new page and update sibling
// return root (0 on failed)
pagenum_t insert_into_internal_after_splitting(int64_t table_id, pagenum_t root,
                                               pagenum_t pagenum,
                                               pagenum_t *sibling, int left_idx,
                                               bpt_key_t key, pagenum_t val);

// delete slot(key, page) from bpt internal page
// return pagenum (0 on failed)
pagenum_t delete_entry_from_internal(int64_t table_id, pagenum_t pagenum,
                                     bpt_key_t key, pagenum_t child);

// delete key from bpt internal page
// handle merge, redistribute
// return root (0 on failed)
pagenum_t delete_from_parent(int64_t table_id, pagenum_t root, pagenum_t parent,
                             bpt_key_t key, pagenum_t pagenum);

// merget neighboring two internal pages
// return root (0 on failed)
pagenum_t merge_internal(int64_t table_id, pagenum_t root,
                         bpt_key_t key_in_parent, pagenum_t pagenum,
                         pagenum_t neighbor_pagenum);

// redistribute internal slots
// move only one slot (on deletion, only one node is needed)
// return root (0 on failed)
pagenum_t redistribute_internal(int64_t table_id, pagenum_t root,
                                bpt_key_t key_in_parent, pagenum_t pagenum,
                                pagenum_t neighbor_pagenum);

// function implements
void move_memory(byte *base, int64_t src_offset, int64_t delta, uint32_t size) {
  memmove(base + (src_offset + delta), base + src_offset, size);
}

leaf_slot_t *leaf_slot_array(bpt_leaf_page_t *page) {
  if (page == NULL) return NULL;
  return (leaf_slot_t *)(page->page.data + kBptPageHeaderSize);
}

internal_slot_t *internal_slot_array(bpt_internal_page_t *page) {
  if (page == NULL) return NULL;
  return (internal_slot_t *)(page->page.data + kBptPageHeaderSize);
}

void init_leaf_page_struct(bpt_leaf_page_t *page, pagenum_t parent_page) {
  if (page == NULL) {
    LOG_ERR(2, "invalid parameters");
    return;
  }
  memset(page, 0, sizeof(bpt_leaf_page_t));
  page->leaf_data.header.is_leaf = 1;
  page->leaf_data.header.num_of_keys = 0;
  page->leaf_data.header.parent_page = parent_page;
  page->leaf_data.header.page_lsn = 0;
  page->leaf_data.free_space = kPageSize - kBptPageHeaderSize;
  page->leaf_data.right_sibling = 0;
}

void init_internal_page_struct(bpt_internal_page_t *page,
                               pagenum_t parent_page) {
  if (page == NULL) {
    LOG_ERR(2, "invalid parameters");
    return;
  }
  memset(page, 0, sizeof(bpt_internal_page_t));
  page->internal_data.header.is_leaf = 0;
  page->internal_data.header.num_of_keys = 0;
  page->internal_data.header.parent_page = parent_page;
  page->internal_data.header.page_lsn = 0;
  page->internal_data.first_child_page = 0;
}

pagenum_t get_neighbor_pagenum(int64_t table_id, pagenum_t parent,
                               pagenum_t pagenum, bpt_key_t *key) {
  auto *page = buffer_get_page_ptr<bpt_internal_page_t>(table_id, parent);
  auto num_of_keys = page->internal_data.header.num_of_keys;
  auto slots = internal_slot_array(page);

  if (num_of_keys == 0) {
    unpin(page);
    LOG_ERR(2, "num_of_keys is zero");
    return 0;
  }

  if (page->internal_data.first_child_page == pagenum) {
    *key = slots[0].key;
    unpin(page);
    return slots[0].pagenum;
  }

  if (slots[0].pagenum == pagenum) {
    *key = slots[0].key;
    unpin(page);
    return page->internal_data.first_child_page;
  }

  for (int i = 1; i < num_of_keys; ++i) {
    if (slots[i].pagenum == pagenum) {
      *key = slots[i].key;
      unpin(page);
      return slots[i - 1].pagenum;
    }
  }

  unpin(page);
  LOG_ERR(2, "there is no page %llu in parent page %llu", pagenum, parent);
  return 0;
}

void set_parent_page(int64_t table_id, pagenum_t pagenum, pagenum_t parent) {
  auto *page = buffer_get_page_ptr<bpt_page_t>(table_id, pagenum);
  page->header.parent_page = parent;
  set_dirty(page);
  unpin(page);
}

bool change_key(int64_t table_id, pagenum_t pagenum, bpt_key_t from,
                bpt_key_t to) {
  auto *page = buffer_get_page_ptr<bpt_internal_page_t>(table_id, pagenum);
  auto num_of_keys = page->internal_data.header.num_of_keys;
  auto slots = internal_slot_array(page);

  for (int i = 0; i < num_of_keys; ++i) {
    if (slots[i].key == from) {
      slots[i].key = to;
      set_dirty(page);
      unpin(page);
      return true;
    }
  }
  unpin(page);
  LOG_ERR(2, "cannot find key %d", from);
  return false;
}

pagenum_t adjust_root(int64_t table_id, pagenum_t root) {
  if (root == 0) return root;

  auto *page = buffer_get_page_ptr<bpt_internal_page_t>(table_id, root);

  // root is not empty
  if (page->internal_data.header.num_of_keys > 0) {
    unpin(page);
    return root;
  }

  // root is empty
  pagenum_t new_root;
  // root is not a leaf
  if (!page->internal_data.header.is_leaf) {
    unpin(page);
    new_root = page->internal_data.first_child_page;
    page = buffer_get_page_ptr<bpt_internal_page_t>(table_id, new_root);
    page->internal_data.header.parent_page = 0;
    set_dirty(page);
    unpin(page);
  } else {
    // root is leaf
    unpin(page);
    new_root = kNullPagenum;
  }

  buffer_free_page(table_id, root);

  return new_root;
}

pagenum_t insert_into_new_root(int64_t table_id, pagenum_t left, bpt_key_t key,
                               pagenum_t right) {
  if (left == 0 || right == 0) {
    LOG_ERR(2, "invalid parameters");
    return 0;
  }

  pagenum_t root = buffer_alloc_page(table_id);
  if (root == 0) {
    LOG_ERR(2, "failed to allocate new page");
    return 0;
  }
  auto *page = buffer_get_page_ptr<bpt_internal_page_t>(table_id, root);
  init_internal_page_struct(page, 0);

  auto slots = internal_slot_array(page);
  slots[0] = {key, right};

  page->internal_data.first_child_page = left;
  page->internal_data.header.num_of_keys = 1;
  set_dirty(page);
  unpin(page);

  set_parent_page(table_id, left, root);
  set_parent_page(table_id, right, root);

  return root;
}

pagenum_t insert_into_parent(int64_t table_id, pagenum_t root, pagenum_t parent,
                             pagenum_t left, bpt_key_t key, pagenum_t right) {
  int left_index;
  if (parent == 0) return insert_into_new_root(table_id, left, key, right);

  auto *parent_page =
      buffer_get_page_ptr<bpt_internal_page_t>(table_id, parent);

  // find left index
  int left_idx = 0;
  auto parent_num_of_keys = parent_page->internal_data.header.num_of_keys;
  auto parent_slots = internal_slot_array(parent_page);
  if (parent_page->internal_data.first_child_page == left)
    left_idx = -1;
  else {
    for (left_idx = 0; left_idx < parent_num_of_keys; ++left_idx) {
      if (parent_slots[left_idx].pagenum == left) break;
    }
    if (left_idx >= parent_num_of_keys) {
      unpin(parent_page);
      LOG_ERR(2, "failed to find left idx");
      return 0;
    }
  }

  // simple case : the new key fits into the node
  if (parent_num_of_keys < kMaxNumInternalPageEntries) {
    if (!insert_into_internal(table_id, parent, parent_page, left_idx, key,
                              right)) {
      unpin(parent_page);
      LOG_ERR(2, "failed to insert into internal page");
      return 0;
    }
    set_dirty(parent_page);
    unpin(parent_page);
    return root;
  }

  // harder case: split a parent node recursively
  unpin(parent_page);
  pagenum_t sibling;
  return insert_into_internal_after_splitting(table_id, root, parent, &sibling,
                                              left_idx, key, right);
}

pagenum_t find_leaf(int64_t table_id, pagenum_t root, bpt_key_t key) {
  if (root == 0) {
    return 0;
  }

  pagenum_t pagenum = root;
  auto *page = buffer_get_page_ptr<bpt_internal_page_t>(table_id, pagenum);

  while (!page->internal_data.header.is_leaf) {
    auto slots = internal_slot_array(page);
    auto num_of_keys = page->internal_data.header.num_of_keys;
    int idx = 0;
    while (idx < num_of_keys && slots[idx].key <= key) ++idx;
    auto old_pagenum = pagenum;
    if (idx == 0)
      pagenum = page->internal_data.first_child_page;
    else
      pagenum = slots[idx - 1].pagenum;
    unpin((page_t *)page);
    page = buffer_get_page_ptr<bpt_internal_page_t>(table_id, pagenum);
  }
  unpin((page_t *)page);
  return pagenum;
}

bool insert_into_leaf(bpt_leaf_page_t *page, bpt_key_t key, uint16_t size,
                      const byte *value) {
  if (page == NULL || value == NULL) {
    LOG_WARN("invalid parameters");
    return false;
  }
  if (size < 46 || size > 108) {
    LOG_WARN("invalid slot data size");
    return false;
  }

  auto num_of_keys = page->leaf_data.header.num_of_keys;
  auto slots = leaf_slot_array(page);

  uint64_t required_space = sizeof(leaf_slot_t) + size;
  if (page->leaf_data.free_space < required_space) {
    LOG_ERR(2, "not enough free space");
    return false;
  }

  // find slot id
  int slotnum;
  for (slotnum = 0; slotnum < num_of_keys; ++slotnum) {
    if (slots[slotnum].key > key) break;
  }

  // calculate slot data offset and move slot data
  uint16_t offset = kPageSize;  // first slot offset
  if (slotnum > 0) {
    offset = slots[slotnum - 1].offset;
  }
  if (num_of_keys > 0) {
    uint16_t last_slot_offset = slots[num_of_keys - 1].offset;
    move_memory(page->page.data, last_slot_offset, -size,
                offset - last_slot_offset);
  }
  offset -= size;

  // update slot data offset
  for (int i = slotnum; i < num_of_keys; ++i) {
    slots[i].offset -= size;
  }

  // insert slot
  for (int i = num_of_keys; i > slotnum; --i) {
    slots[i] = slots[i - 1];
  }
  slots[slotnum] = {key, size, offset, 0};

  // insert slot data
  memcpy(page->page.data + offset, value, size);

  // update header
  page->leaf_data.header.num_of_keys += 1;
  page->leaf_data.free_space -= required_space;
  return true;
}

// insert new slot into bpt leaf page
// create new page and update sibling
// return root pagenum (0 on failed)
pagenum_t insert_into_leaf_after_splitting(int64_t table_id, pagenum_t root,
                                           pagenum_t pagenum,
                                           pagenum_t *sibling, bpt_key_t key,
                                           uint16_t size, const byte *value) {
  if (sibling == NULL || value == NULL) {
    LOG_ERR(2, "invalid parameters");
    return 0;
  }
  if (size < 46 || size > 108) {
    LOG_ERR(2, "invalid slot data size");
    return 0;
  }

  auto *page = buffer_get_page_ptr<bpt_leaf_page_t>(table_id, pagenum);
  auto parent_page = page->leaf_data.header.parent_page;
  auto old_num_of_keys = page->leaf_data.header.num_of_keys;

  // check if leaf page is full
  if (page->leaf_data.free_space >= sizeof(leaf_slot_t) + size) {
    unpin(page);
    LOG_WARN("tried to split but page is not full, free: %u, required: %u",
             page->leaf_data.free_space, sizeof(leaf_slot_t) + size);
    return 0;
  }

  // create new leaf page]
  *sibling = buffer_alloc_page(table_id);
  if (*sibling == 0) {
    unpin(page);
    LOG_ERR(2, "failed to allocate new sibling page");
    return 0;
  }
  auto *new_page = buffer_get_page_ptr<bpt_leaf_page_t>(table_id, *sibling);
  init_leaf_page_struct(new_page, parent_page);

  // get slot arrays
  auto slots = leaf_slot_array(page);
  auto new_slots = leaf_slot_array(new_page);

  // find insertion index
  int insert_idx;
  for (insert_idx = 0; insert_idx < old_num_of_keys; ++insert_idx) {
    if (slots[insert_idx].key > key) break;
  }

  // initialize temp_slots array
  auto new_num_of_keys = old_num_of_keys + 1;
  auto temp_slots =
      (leaf_slot_t *)malloc(new_num_of_keys * sizeof(leaf_slot_t));
  if (temp_slots == NULL) {
    unpin(page);
    unpin(new_page);
    LOG_ERR(2, "failed to allocate temp_slots array");
    return 0;
  }
  for (int i = 0, j = 0; i < old_num_of_keys; ++i, ++j) {
    if (j == insert_idx) ++j;
    temp_slots[j] = slots[i];
  }
  temp_slots[insert_idx] = {key, size, 0};

  // find split point
  uint64_t space = 0, split = 0;
  const uint64_t kThreshold = (kPageSize - kBptPageHeaderSize) / 2;
  for (split = 0; split < new_num_of_keys; ++split) {
    space += sizeof(leaf_slot_t) + temp_slots[split].size;
    if (space >= kThreshold) break;
  }

  // split into two leaf page
  // create updated page (alter page)
  bpt_leaf_page_t upd_page;
  init_leaf_page_struct(&upd_page, parent_page);
  auto upd_slots = leaf_slot_array(&upd_page);

  // insert into updated page (alter page)
  uint16_t data_offset = kPageSize;
  for (int i = 0; i <= split; ++i) {
    upd_page.leaf_data.free_space -= sizeof(leaf_slot_t) + temp_slots[i].size;
    upd_page.leaf_data.header.num_of_keys += 1;

    data_offset -= temp_slots[i].size;
    upd_slots[i] = temp_slots[i];
    upd_slots[i].offset = data_offset;

    if (i == insert_idx)
      memcpy(upd_page.page.data + data_offset, value, temp_slots[i].size);
    else
      memcpy(upd_page.page.data + data_offset,
             page->page.data + temp_slots[i].offset, temp_slots[i].size);
  }

  // insert into new page (sibling page)
  data_offset = kPageSize;
  for (int i = split + 1, j = 0; i < new_num_of_keys; ++i, ++j) {
    new_page->leaf_data.free_space -= sizeof(leaf_slot_t) + temp_slots[i].size;
    new_page->leaf_data.header.num_of_keys += 1;

    data_offset -= temp_slots[i].size;
    new_slots[j] = temp_slots[i];
    new_slots[j].offset = data_offset;

    if (i == insert_idx)
      memcpy(new_page->page.data + data_offset, value, temp_slots[i].size);
    else
      memcpy(new_page->page.data + data_offset,
             page->page.data + temp_slots[i].offset, temp_slots[i].size);
  }

  // free allocated resources
  free(temp_slots);

  // update sibling pagenum
  upd_page.leaf_data.right_sibling = *sibling;
  new_page->leaf_data.right_sibling = page->leaf_data.right_sibling;

  // write
  memcpy(page->page.data, upd_page.page.data, sizeof(upd_page));
  set_dirty(page);
  set_dirty(new_page);
  unpin(page);
  unpin(new_page);

  auto mid_key = new_slots[0].key;
  return insert_into_parent(table_id, root, parent_page, pagenum, mid_key,
                            *sibling);
}

pagenum_t delete_entry_from_leaf(bpt_leaf_page_t *leaf_page, pagenum_t pagenum,
                                 bpt_key_t key) {
  auto num_of_keys = leaf_page->leaf_data.header.num_of_keys;
  auto slots = leaf_slot_array(leaf_page);

  // find slot with given key
  int slotnum;
  for (slotnum = 0; slotnum < num_of_keys; ++slotnum) {
    if (slots[slotnum].key == key) break;
  }
  if (slotnum >= num_of_keys) {
    LOG_WARN("failed to find slot(key=%lld) from page %llu", key, pagenum);
    return 0;
  }
  auto freed_space = sizeof(leaf_slot_t) + slots[slotnum].size;

  // move slot data
  leaf_slot_t last_slot = slots[num_of_keys - 1];
  move_memory(leaf_page->page.data, last_slot.offset, slots[slotnum].size,
              slots[slotnum].offset - last_slot.offset);
  memset(leaf_page->page.data + last_slot.offset, 0, slots[slotnum].size);

  // update slot metadata whose data are moved
  for (int i = 0; i < num_of_keys; ++i) {
    if (slots[i].offset < slots[slotnum].offset) {
      slots[i].offset += slots[slotnum].size;
    }
  }

  // move all slot metadata placed after this slot
  for (int i = slotnum; i < num_of_keys - 1; ++i) {
    slots[i] = slots[i + 1];
  }
  slots[num_of_keys - 1] = {0, 0, 0};

  // update header
  leaf_page->leaf_data.header.num_of_keys -= 1;
  leaf_page->leaf_data.free_space += freed_space;

  set_dirty(leaf_page);
  return pagenum;
}

pagenum_t delete_from_leaf(int64_t table_id, pagenum_t root, pagenum_t pagenum,
                           bpt_key_t key) {
  auto *page = buffer_get_page_ptr<bpt_leaf_page_t>(table_id, pagenum);
  pagenum = delete_entry_from_leaf(page, pagenum, key);
  if (pagenum == 0) {
    unpin(page);
    return 0;
  }

  // if delete from the root
  if (root == pagenum) {
    unpin(page);
    return adjust_root(table_id, root);
  }

  // if free space is less than threshold, do nothing
  auto free_space = page->leaf_data.free_space;
  if (free_space < kMergeOrDistributeThreshold) {
    unpin(page);
    return root;
  }

  bpt_key_t key_in_parent;
  auto neighbor_pagenum = get_neighbor_pagenum(
      table_id, page->leaf_data.header.parent_page, pagenum, &key_in_parent);
  if (neighbor_pagenum == 0) {
    unpin(page);
    LOG_ERR(2, "failed to find neighbor page");
    return 0;
  }

  auto *neighbor_page =
      buffer_get_page_ptr<bpt_leaf_page_t>(table_id, neighbor_pagenum);

  if (neighbor_page->leaf_data.header.parent_page !=
      page->leaf_data.header.parent_page) {
    unpin(page);
    unpin(neighbor_page);
    LOG_ERR(2, "parent is not same");
    return 0;
  }

  uint64_t used_space = (kPageSize - kBptPageHeaderSize) - free_space;

  // if there is enough space, then merge
  if (used_space <= neighbor_page->leaf_data.free_space) {
    unpin(page);
    unpin(neighbor_page);
    return merge_leaf(table_id, root, key_in_parent, pagenum, neighbor_pagenum);
  }
  // if there is no enough space, then redistribute
  else {
    unpin(page);
    unpin(neighbor_page);
    return redistribute_leaf(table_id, root, key_in_parent, pagenum,
                             neighbor_pagenum);
  }
}

pagenum_t merge_leaf(int64_t table_id, pagenum_t root, bpt_key_t key_in_parent,
                     pagenum_t pagenum, pagenum_t neighbor_pagenum) {
  auto *page = buffer_get_page_ptr<bpt_leaf_page_t>(table_id, pagenum);
  auto *neighbor =
      buffer_get_page_ptr<bpt_leaf_page_t>(table_id, neighbor_pagenum);

  auto slots = leaf_slot_array(page);
  auto neig_slots = leaf_slot_array(neighbor);

  auto page_is_left = true;
  auto left_pagenum = pagenum, right_pagenum = neighbor_pagenum;
  bpt_leaf_page_t *left = page, *right = neighbor;
  auto left_slots = slots, right_slots = neig_slots;
  if (right_slots[0].key < left_slots[0].key) {
    std::swap(left_pagenum, right_pagenum);
    std::swap(left, right);
    std::swap(left_slots, right_slots);
    page_is_left = false;
  }
  auto left_num_of_keys = left->leaf_data.header.num_of_keys;
  auto right_num_of_keys = right->leaf_data.header.num_of_keys;
  auto parent_pagenum = right->leaf_data.header.parent_page;

  // copy right page's slots into left page
  for (int i = 0; i < right_num_of_keys; ++i) {
    if (!insert_into_leaf(left, right_slots[i].key, right_slots[i].size,
                          right->page.data + right_slots[i].offset)) {
      unpin(page);
      unpin(neighbor);
      LOG_ERR(2, "failed to insert");
      return 0;
    }
  }

  // update right sibling
  left->leaf_data.right_sibling = right->leaf_data.right_sibling;

  // free right page and delete from parent
  set_dirty(left);
  unpin(page);
  unpin(neighbor);
  buffer_free_page(table_id, right_pagenum);
  return delete_from_parent(table_id, root, parent_pagenum, key_in_parent,
                            right_pagenum);
}

pagenum_t redistribute_leaf(int64_t table_id, pagenum_t root,
                            bpt_key_t key_in_parent, pagenum_t pagenum,
                            pagenum_t neighbor_pagenum) {
  auto *page = buffer_get_page_ptr<bpt_leaf_page_t>(table_id, pagenum);
  auto *neighbor =
      buffer_get_page_ptr<bpt_leaf_page_t>(table_id, neighbor_pagenum);
  bpt_leaf_page_t upd_neighbor;
  auto parent_page = page->leaf_data.header.parent_page;
  init_leaf_page_struct(&upd_neighbor, parent_page);
  upd_neighbor.leaf_data.right_sibling = neighbor->leaf_data.right_sibling;

  auto slots = leaf_slot_array(page);
  auto neig_slots = leaf_slot_array(neighbor);
  auto upd_neig_slots = leaf_slot_array(&upd_neighbor);

  auto page_is_left = true;
  bpt_leaf_page_t *left = page, *right = neighbor;
  auto left_slots = slots, right_slots = neig_slots;
  if (right_slots[0].key < left_slots[0].key) {
    std::swap(left, right);
    std::swap(left_slots, right_slots);
    page_is_left = false;
  }
  auto left_num_of_keys = left->leaf_data.header.num_of_keys;
  auto right_num_of_keys = right->leaf_data.header.num_of_keys;

  bpt_key_t new_key_in_parent;
  // move slots from neighbor to page
  if (page_is_left) {
    // move slots
    int right_idx = 0;
    while (left->leaf_data.free_space >= kMergeOrDistributeThreshold &&
           right_idx < right_num_of_keys) {
      auto slot = right_slots[right_idx++];
      // cause maximum slot.size is 108, space is always enough
      if (!insert_into_leaf(left, slot.key, slot.size,
                            right->page.data + slot.offset)) {
        unpin(page);
        unpin(neighbor);
        LOG_ERR(2, "failed to insert slot into left page %llu", pagenum);
        return 0;
      }
    }

    // rebuild neighbor
    uint16_t offset = kPageSize;
    for (int i = right_idx, j = 0; i < right_num_of_keys; ++i, ++j) {
      auto slot = right_slots[i];
      offset -= slot.size;
      upd_neig_slots[j] = {slot.key, slot.size, offset};
      memcpy(upd_neighbor.page.data + offset, right->page.data + slot.offset,
             slot.size);
      upd_neighbor.leaf_data.header.num_of_keys += 1;
      upd_neighbor.leaf_data.free_space -= sizeof(leaf_slot_t) + slot.size;
    }

    new_key_in_parent = upd_neig_slots[0].key;
  } else {  // page is right
    // move slots
    int left_idx = left_num_of_keys - 1;
    while (right->leaf_data.free_space >= kMergeOrDistributeThreshold &&
           left_idx >= 0) {
      auto slot = left_slots[left_idx--];
      if (!insert_into_leaf(right, slot.key, slot.size,
                            left->page.data + slot.offset)) {
        unpin(page);
        unpin(neighbor);
        LOG_ERR(2, "failed to insert slot into right page %llu", pagenum);
        return 0;
      }
    }

    // rebuild neighbor
    uint16_t offset = kPageSize;
    for (int i = 0; i <= left_idx; ++i) {
      auto slot = left_slots[i];
      offset -= slot.size;
      upd_neig_slots[i] = {slot.key, slot.size, offset};
      memcpy(upd_neighbor.page.data + offset, left->page.data + slot.offset,
             slot.size);
      upd_neighbor.leaf_data.header.num_of_keys += 1;
      upd_neighbor.leaf_data.free_space -= sizeof(leaf_slot_t) + slot.size;
    }

    new_key_in_parent = right_slots[0].key;
  }

  memcpy(neighbor->page.data, upd_neighbor.page.data, sizeof(upd_neighbor));
  set_dirty(page);
  set_dirty(neighbor);
  unpin(page);
  unpin(neighbor);
  change_key(table_id, parent_page, key_in_parent, new_key_in_parent);

  return root;
}

bool insert_into_internal(int64_t table_id, pagenum_t pagenum,
                          bpt_internal_page_t *page, int left_idx,
                          bpt_key_t key, pagenum_t val) {
  if (page == NULL) {
    LOG_ERR(2, "invalid parameters");
    return false;
  }

  auto num_of_keys = page->internal_data.header.num_of_keys;
  auto slots = internal_slot_array(page);

  if (num_of_keys >= kMaxNumInternalPageEntries) {
    LOG_ERR(2, "not enough space");
    return false;
  }

  if (left_idx >= (int64_t)num_of_keys) {
    LOG_ERR(2, "invalid left idx");
    return false;
  }

  for (int i = num_of_keys - 1; i > left_idx; --i) {
    slots[i + 1] = slots[i];
  }
  slots[left_idx + 1] = {key, val};

  // update header
  page->internal_data.header.num_of_keys += 1;

  // set paretn_page of newly inserted page
  set_parent_page(table_id, val, pagenum);

  return true;
}

pagenum_t insert_into_internal_after_splitting(int64_t table_id, pagenum_t root,
                                               pagenum_t pagenum,
                                               pagenum_t *sibling, int left_idx,
                                               bpt_key_t key, pagenum_t val) {
  if (sibling == NULL) {
    LOG_ERR(2, "invalid parameters");
    return 0;
  }

  auto *page = buffer_get_page_ptr<bpt_internal_page_t>(table_id, pagenum);
  auto parent_page = page->internal_data.header.parent_page;
  auto old_num_of_keys = page->internal_data.header.num_of_keys;

  // check if page is full
  if (old_num_of_keys < kMaxNumInternalPageEntries) {
    unpin(page);
    LOG_WARN("tried to split but page is not full");
    return 0;
  }

  // create new internal page
  *sibling = buffer_alloc_page(table_id);
  if (*sibling == 0) {
    unpin(page);
    LOG_ERR(2, "failed to allocate new sibling page");
    return 0;
  }
  auto *new_page = buffer_get_page_ptr<bpt_internal_page_t>(table_id, *sibling);
  init_internal_page_struct(new_page, parent_page);

  // get slot arrays
  auto slots = internal_slot_array(page);
  auto new_slots = internal_slot_array(new_page);

  // initialize temp_slots array
  auto new_num_of_keys = old_num_of_keys + 1;
  auto temp_slots =
      (internal_slot_t *)malloc(new_num_of_keys * sizeof(internal_slot_t));
  if (temp_slots == NULL) {
    unpin(page);
    unpin(new_page);
    LOG_ERR(2, "failed to allocate temp_slots array");
    return 0;
  }
  for (int i = 0, j = 0; i < old_num_of_keys; ++i, ++j) {
    if (j == left_idx + 1) ++j;
    temp_slots[j] = slots[i];
  }
  temp_slots[left_idx + 1] = {key, val};
  set_parent_page(table_id, val, pagenum);

  // calculate split point
  auto split = new_num_of_keys / 2 + new_num_of_keys % 2;

  // split into two internal page
  // create updated page (alter page)
  bpt_internal_page_t upd_page;
  init_internal_page_struct(&upd_page, parent_page);
  auto upd_slots = internal_slot_array(&upd_page);

  // insert into updated page (alter page)
  upd_page.internal_data.first_child_page =
      page->internal_data.first_child_page;
  int i = 0;
  for (i = 0; i < split; ++i) {
    upd_page.internal_data.header.num_of_keys += 1;
    upd_slots[i] = temp_slots[i];
  }

  // insert into new page (sibling page)
  new_page->internal_data.first_child_page = temp_slots[i].pagenum;
  set_parent_page(table_id, temp_slots[i].pagenum, *sibling);
  auto mid_key = temp_slots[i++].key;
  for (int j = 0; i < new_num_of_keys; ++i, ++j) {
    new_page->internal_data.header.num_of_keys += 1;
    new_slots[j] = temp_slots[i];
    set_parent_page(table_id, temp_slots[i].pagenum, *sibling);
  }

  // free allocated page
  free(temp_slots);

  // write
  memcpy(page->page.data, upd_page.page.data, sizeof(upd_page));
  set_dirty(page);
  set_dirty(new_page);
  unpin(page);
  unpin(new_page);

  return insert_into_parent(table_id, root, parent_page, pagenum, mid_key,
                            *sibling);
}

pagenum_t delete_entry_from_internal(bpt_internal_page_t *page,
                                     pagenum_t pagenum, bpt_key_t key,
                                     pagenum_t child) {
  auto num_of_keys = page->internal_data.header.num_of_keys;
  auto slots = internal_slot_array(page);

  // find key idx
  int key_idx;
  for (key_idx = 0; key_idx < num_of_keys; ++key_idx) {
    if (slots[key_idx].key == key) break;
  }
  if (key_idx >= num_of_keys) {
    LOG_WARN("failed to find a slot(key=%d, page: %llu)", key, child);
    return 0;
  }

  // check if removing child is right
  auto removing_right = true;
  if (key_idx == 0 && page->internal_data.first_child_page == child)
    removing_right = false;
  if (key_idx > 0 && slots[key_idx - 1].pagenum == child)
    removing_right = false;

  // remove key and child pagenum
  if (removing_right) {
    if (slots[key_idx].pagenum != child) {
      LOG_WARN("failed to find a slot(key=%d, page: %llu)", key, child);
      return 0;
    }
  } else {
    if (key_idx == 0)
      page->internal_data.first_child_page = slots[0].pagenum;
    else
      slots[key_idx - 1].pagenum = slots[key_idx].pagenum;
  }
  for (int i = key_idx; i < num_of_keys - 1; ++i) {
    slots[i] = slots[i + 1];
  }
  slots[num_of_keys - 1] = {0, 0};
  page->internal_data.header.num_of_keys -= 1;

  set_dirty(page);
  return pagenum;
}

pagenum_t delete_from_parent(int64_t table_id, pagenum_t root,
                             pagenum_t pagenum, bpt_key_t key, pagenum_t val) {
  auto *page = buffer_get_page_ptr<bpt_internal_page_t>(table_id, pagenum);
  pagenum = delete_entry_from_internal(page, pagenum, key, val);
  if (pagenum == 0) {
    unpin(page);
    LOG_ERR(2, "failed to delete entry from internal page");
    return 0;
  }

  // if delete from the root
  if (root == pagenum) {
    unpin(page);
    return adjust_root(table_id, root);
  }

  auto num_of_keys = page->internal_data.header.num_of_keys;

  // if page has enough keys
  const auto min_keys =
      kMaxNumInternalPageEntries / 2 + kMaxNumInternalPageEntries % 2 - 1;
  if (num_of_keys >= min_keys) {
    unpin(page);
    return root;
  }

  bpt_key_t key_in_parent;
  auto neighbor_pagenum =
      get_neighbor_pagenum(table_id, page->internal_data.header.parent_page,
                           pagenum, &key_in_parent);
  if (neighbor_pagenum == 0) {
    unpin(page);
    LOG_ERR(2, "failed to find neighbor page");
    return 0;
  }

  auto *neighbor_page =
      buffer_get_page_ptr<bpt_internal_page_t>(table_id, neighbor_pagenum);
  auto neig_num_of_keys = neighbor_page->internal_data.header.num_of_keys;

  if (neighbor_page->internal_data.header.parent_page !=
      page->internal_data.header.parent_page) {
    unpin(page);
    unpin(neighbor_page);
    LOG_ERR(2, "parent is not same");
    return 0;
  }

  // if there is enough space, then merge
  if (num_of_keys + neig_num_of_keys < kMaxNumInternalPageEntries) {
    unpin(page);
    unpin(neighbor_page);
    return merge_internal(table_id, root, key_in_parent, pagenum,
                          neighbor_pagenum);
  }
  // if there is no enough space, then redistribute
  else {
    unpin(page);
    unpin(neighbor_page);
    return redistribute_internal(table_id, root, key_in_parent, pagenum,
                                 neighbor_pagenum);
  }

  return root;
}

pagenum_t merge_internal(int64_t table_id, pagenum_t root,
                         bpt_key_t key_in_parent, pagenum_t pagenum,
                         pagenum_t neighbor_pagenum) {
  auto *page = buffer_get_page_ptr<bpt_internal_page_t>(table_id, pagenum);
  auto *neighbor =
      buffer_get_page_ptr<bpt_internal_page_t>(table_id, neighbor_pagenum);
  auto parent_pagenum = page->internal_data.header.parent_page;

  auto page_is_left = true;
  auto left_pagenum = pagenum, right_pagenum = neighbor_pagenum;
  auto left = page, right = neighbor;
  auto left_slots = internal_slot_array(left);
  auto right_slots = internal_slot_array(right);
  if (right_slots[0].key < left_slots[0].key) {
    std::swap(left, right);
    std::swap(left_slots, right_slots);
    std::swap(left_pagenum, right_pagenum);
    page_is_left = false;
  }
  auto left_num_of_keys = left->internal_data.header.num_of_keys;
  auto right_num_of_keys = right->internal_data.header.num_of_keys;

  // insert new slot(key=key_in_parent, page=right.first) into left
  int left_idx = left_num_of_keys - 1;
  if (!insert_into_internal(table_id, left_pagenum, left, left_idx++,
                            key_in_parent,
                            right->internal_data.first_child_page)) {
    unpin(page);
    unpin(neighbor);
    LOG_ERR(2, "failed to insert");
    return 0;
  }

  // insert right's slots into left
  for (int i = 0; i < right_num_of_keys; ++i) {
    auto slot = right_slots[i];
    if (!insert_into_internal(table_id, left_pagenum, left, left_idx++,
                              slot.key, slot.pagenum)) {
      unpin(page);
      unpin(neighbor);
      LOG_ERR(2, "failed to insert");
      return 0;
    }
  }

  set_dirty(left);
  unpin(page);
  unpin(neighbor);
  buffer_free_page(table_id, right_pagenum);
  return delete_from_parent(table_id, root, parent_pagenum, key_in_parent,
                            right_pagenum);
}

pagenum_t redistribute_internal(int64_t table_id, pagenum_t root,
                                bpt_key_t key_in_parent, pagenum_t pagenum,
                                pagenum_t neighbor_pagenum) {
  auto *page = buffer_get_page_ptr<bpt_internal_page_t>(table_id, pagenum);
  auto *neighbor =
      buffer_get_page_ptr<bpt_internal_page_t>(table_id, neighbor_pagenum);
  auto parent_pagenum = page->internal_data.header.parent_page;

  auto page_is_left = true;
  auto left_pagenum = pagenum, right_pagenum = neighbor_pagenum;
  auto left = page, right = neighbor;
  auto left_slots = internal_slot_array(left);
  auto right_slots = internal_slot_array(right);
  if (right_slots[0].key < left_slots[0].key) {
    std::swap(left_pagenum, right_pagenum);
    std::swap(left, right);
    std::swap(left_slots, right_slots);
    page_is_left = false;
  }
  auto left_num_of_keys = left->internal_data.header.num_of_keys;
  auto right_num_of_keys = right->internal_data.header.num_of_keys;

  if (page_is_left) {
    // insert new slot(key = parent's key, page=right's first) into left
    auto right_first_page = right->internal_data.first_child_page;
    if (!insert_into_internal(table_id, left_pagenum, left,
                              left_num_of_keys - 1, key_in_parent,
                              right_first_page)) {
      unpin(page);
      unpin(neighbor);
      LOG_ERR(2, "failed to insert");
      return 0;
    }

    // change key in parent into right's first key
    change_key(table_id, parent_pagenum, key_in_parent, right_slots[0].key);

    // move right slots
    right->internal_data.first_child_page = right_slots[0].pagenum;
    for (int i = 0; i < right_num_of_keys - 1; ++i) {
      right_slots[i] = right_slots[i + 1];
    }
    right_slots[right_num_of_keys - 1] = {0, 0};
    right->internal_data.header.num_of_keys -= 1;

  } else {  // page is right
    // insert new slot(key = parent's key, page=left's last) into right
    auto left_last_slot = left_slots[left_num_of_keys - 1];
    for (int i = right_num_of_keys - 1; i >= 0; --i) {
      right_slots[i + 1] = right_slots[i];
    }
    right_slots[0] = {key_in_parent, right->internal_data.first_child_page};
    right->internal_data.first_child_page = left_last_slot.pagenum;
    set_parent_page(table_id, left_last_slot.pagenum, right_pagenum);
    right->internal_data.header.num_of_keys += 1;

    // change key in parent into left's last key
    change_key(table_id, parent_pagenum, key_in_parent, left_last_slot.key);

    // clean left slots
    left_slots[left_num_of_keys - 1] = {0, 0};
    left->internal_data.header.num_of_keys -= 1;
  }

  set_dirty(page);
  set_dirty(neighbor);
  unpin(page);
  unpin(neighbor);

  return root;
}

// API functions
bool bpt_find(int64_t table_id, pagenum_t root, bpt_key_t key, uint16_t *size,
              byte *value, int trx_id, lock_t *lock) {
  auto leaf_pagenum = find_leaf(table_id, root, key);
  if (leaf_pagenum == 0) {
    return false;
  }

  auto *page = buffer_get_page_ptr<bpt_leaf_page_t>(table_id, leaf_pagenum);
  if (lock == NULL &&
      trx_id > 0) {  // acquire record lock before getting page latch
    int waited = false;
    auto *new_lock = lock_acquire((bpt_page_t **)&page, table_id, leaf_pagenum,
                                  key, trx_id, S_LOCK, &waited);
    if (new_lock == NULL) {
      unpin(page);
      return false;
    } else if (waited) {
      // do it again
      auto *header =
          buffer_get_page_ptr<header_page_t>(table_id, kHeaderPagenum);
      root = header->header.root_page_number;
      unpin(header);
      return bpt_find(table_id, root, key, size, value, trx_id, new_lock);
    }
  }
  auto slots = leaf_slot_array(page);
  auto num_of_keys = page->leaf_data.header.num_of_keys;
  for (int i = 0; i < num_of_keys; ++i) {
    if (slots[i].key == key) {
      if (size != NULL) *size = slots[i].size;
      if (value != NULL)
        memcpy(value, page->page.data + slots[i].offset, slots[i].size);
      unpin((page_t *)page);
      return true;
    }
  }
  unpin((page_t *)page);
  return false;
}

bool bpt_update(int64_t table_id, pagenum_t root, bpt_key_t key, byte *value,
                uint16_t new_val_size, uint16_t *old_val_size, int trx_id,
                lock_t *lock) {
  auto leaf_pagenum = find_leaf(table_id, root, key);
  if (leaf_pagenum == 0) return false;

  auto *page = buffer_get_page_ptr<bpt_leaf_page_t>(table_id, leaf_pagenum);
  trx_t *trx = NULL;
  if (lock != NULL) {
    trx = get_trx(lock);
  } else if (trx_id > 0) {  // acquire record lock before getting page latch
    int waited = false;
    auto *new_lock = lock_acquire((bpt_page_t **)&page, table_id, leaf_pagenum,
                                  key, trx_id, X_LOCK, &waited);
    if (new_lock == NULL) {
      unpin(page);
      return false;
    } else if (waited) {
      // do it again
      auto *header =
          buffer_get_page_ptr<header_page_t>(table_id, kHeaderPagenum);
      root = header->header.root_page_number;
      unpin(header);
      return bpt_update(table_id, root, key, value, new_val_size, old_val_size,
                        trx_id, new_lock);
    }
    trx = get_trx(new_lock);
  }

  auto slots = leaf_slot_array(page);
  auto num_of_keys = page->leaf_data.header.num_of_keys;
  for (int i = 0; i < num_of_keys; ++i) {
    if (slots[i].key == key) {
      log_record_t *rec = NULL;
      if (trx != NULL && value != NULL) {
        rec = create_log_update(trx, table_id, leaf_pagenum, slots[i].offset,
                                new_val_size, page->page.data + slots[i].offset,
                                value);
        if (rec == NULL) {
          LOG_ERR(2, "failed to make update log");
          return false;
        }
      }
      if (old_val_size != NULL) *old_val_size = slots[i].size;
      if (value != NULL) {
        auto copy_size =
            new_val_size < slots[i].size ? new_val_size : slots[i].size;
        memcpy(page->page.data + slots[i].offset, value, copy_size);
        set_dirty(page);
        if (trx != NULL) {
          if (push_into_log_buffer(rec)) {
            free(rec);
            LOG_ERR(2, "failed to push log into log buffer");
            return false;
          }
          if (trx_log_update(trx, rec)) {
            free(rec);
            LOG_ERR(2, "failed to add log into the trx");
            return false;
          }
          page->leaf_data.header.page_lsn = rec->lsn;
          set_dirty(page);
        }
      }
      if (rec != NULL) free(rec);

      unpin(page);
      return true;
    }
  }
  unpin(page);
  return false;
}
pagenum_t bpt_insert(int64_t table_id, pagenum_t root, bpt_key_t key,
                     uint16_t size, const byte *value) {
  if (value == NULL) {
    LOG_ERR(2, "invalid parameters");
    return 0;
  }
  if (size < 46 || size > 108) {
    LOG_ERR(2, "invalid slot data size");
    return 0;
  }

  // duplicates are not allowed
  if (bpt_find(table_id, root, key, NULL, NULL, -1)) {
    LOG_WARN("%lld already exists", key);
    return 0;
  }

  auto required_space = sizeof(leaf_slot_t) + size;

  // if there is no root, then create new root
  if (root == 0) {
    root = buffer_alloc_page(table_id);
    if (root == 0) {
      LOG_ERR(2, "failed to allocate new page");
      return 0;
    }

    auto *page = buffer_get_page_ptr<bpt_leaf_page_t>(table_id, root);
    init_leaf_page_struct(page, 0);
    auto slots = leaf_slot_array(page);

    uint16_t offset = kPageSize - size;
    slots[0] = {key, size, offset};
    memcpy(page->page.data + offset, value, size);
    page->leaf_data.free_space -= required_space;
    page->leaf_data.header.num_of_keys += 1;

    set_dirty(page);
    unpin(page);
    return root;
  }

  auto leaf_pagenum = find_leaf(table_id, root, key);
  if (leaf_pagenum == 0) {
    return 0;
  }

  auto *page = buffer_get_page_ptr<bpt_leaf_page_t>(table_id, leaf_pagenum);

  // leaf has enough space
  if (page->leaf_data.free_space >= required_space) {
    if (!insert_into_leaf(page, key, size, value)) {
      unpin(page);
      LOG_ERR(2, "failed to insert into leaf");
      return 0;
    }
    set_dirty(page);
    unpin(page);
    return root;
  }

  // leaf doesn't have enough space
  unpin(page);
  pagenum_t sibling;
  return insert_into_leaf_after_splitting(table_id, root, leaf_pagenum,
                                          &sibling, key, size, value);
}

pagenum_t bpt_delete(int64_t table_id, pagenum_t root, bpt_key_t key) {
  auto leaf_pagenum = find_leaf(table_id, root, key);
  if (leaf_pagenum == 0) return 0;

  return delete_from_leaf(table_id, root, leaf_pagenum, key);
}
