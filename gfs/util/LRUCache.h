#ifndef STARKNOWLEDGEGRAPHDATABASE_SIMPLELRUCACHE_H
#define STARKNOWLEDGEGRAPHDATABASE_SIMPLELRUCACHE_H

#include <cstdint>
#include <list>
#include <mutex>
#include <map>
#include <unordered_map>

template<typename KeyType, typename ValueType>
class LruCache {
private:
    typedef typename std::pair<KeyType, ValueType> KeyValuePairType;
    typedef typename std::list<KeyValuePairType>::iterator ListIteratorType;
//    typedef typename std::unordered_map<KeyType, ListIteratorType>::iterator MapIteratorType;
    typedef typename std::map<KeyType, ListIteratorType>::iterator MapIteratorType;

public:
    LruCache(size_t capacity=10);
    inline void SetCapacity(size_t capacity) {
        // TODO 暂时不处理下面的情况
        // TODO 若一开始设置容量比较大. 把一些对象存储到cache中之后, 再调用此函数把容量缩减, 应该把尾部的数据全部淘汰?
        capacity_ = capacity;
    }
    bool isExist(const KeyType& key) const {
        // init lock
        std::lock_guard<std::mutex> lock(mutex_);
        return cache_items_map_.find(key) != cache_items_map_.end();
    }
    void Set(const KeyType& key, const ValueType& value, ValueType *eliminatedValue);
    bool Get(const KeyType& key, ValueType* value);
    void Erase(const KeyType& key, ValueType *eliminatedValue);

private:
    mutable std::mutex mutex_;
    size_t capacity_;
    std::list<KeyValuePairType> cache_items_list_;
//    std::unordered_map<KeyType, ListIteratorType> cache_items_map_;
    std::map<KeyType, ListIteratorType> cache_items_map_;
};

template<typename KeyType, typename ValueType>
LruCache<KeyType, ValueType>::LruCache(size_t capacity)
        : capacity_(capacity) {
}

template<typename KeyType, typename ValueType>
void LruCache<KeyType, ValueType>::Set(const KeyType& key, const ValueType& value, ValueType *eliminatedValue) {
    // init lock
    std::lock_guard<std::mutex> lock(mutex_);

    MapIteratorType it = cache_items_map_.find(key);
    if (it != cache_items_map_.end()) {
        // key 已存在, update value
        cache_items_list_.emplace(it->second, key, value);
        // 放到链表头
        cache_items_list_.splice(cache_items_list_.begin(), cache_items_list_, it->second);
        // 调整map的指针
        it->second = cache_items_list_.begin();
    } else {
        // 若 Cache 满了, 淘汰链表最后一个元素
        if (cache_items_map_.size() >= capacity_) {
            cache_items_map_.erase(cache_items_list_.back().first);
            if (eliminatedValue != nullptr) {
                *eliminatedValue = std::move(cache_items_list_.back().second);
            }
            cache_items_list_.pop_back();
        }
        // 放到链表头
        cache_items_list_.emplace_front(key, value);
        // 调整map的指针
        cache_items_map_[key] = cache_items_list_.begin();
    }
}

template<typename KeyType, typename ValueType>
bool LruCache<KeyType, ValueType>::Get(const KeyType& key, ValueType* value) {
    // init lock
    std::lock_guard<std::mutex> lock(mutex_);

    MapIteratorType it = cache_items_map_.find(key);
    if (it == cache_items_map_.end()) {
        // Cache miss
        return false;
    } else {
        // Cache hit, 调整 value 放到链表头
        cache_items_list_.splice(cache_items_list_.begin(), cache_items_list_, it->second);
        *value = it->second->second;
        return true;
    }
}

template <typename KeyType, typename ValueType>
void LruCache<KeyType, ValueType>::Erase(const KeyType &key, ValueType *eliminatedValue) {
    std::lock_guard<std::mutex> lock(mutex_);

    MapIteratorType it = cache_items_map_.find(key);
    if (it == cache_items_map_.end()) {
        return;
    }

    // remove cache item
    *eliminatedValue = std::move(it->second->second);
    cache_items_list_.erase(it->second);
    cache_items_map_.erase(it);
}

#endif //STARKNOWLEDGEGRAPHDATABASE_SIMPLELRUCACHE_H
