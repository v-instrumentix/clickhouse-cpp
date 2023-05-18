#include "ix-json.h"
#include "utils.h"

#include "../base/wire_format.h"

namespace {

constexpr size_t DEFAULT_BLOCK_SIZE = 4096;

template <typename Container>
size_t ComputeTotalSize(const Container & strings, size_t begin = 0, size_t len = -1) {
    size_t result = 0;
    if (begin < strings.size()) {
        len = std::min(len, strings.size() - begin);

        for (size_t i = begin; i < begin + len; ++i)
            result += strings[i].size();
    }

    return result;
}

}

namespace clickhouse {

struct ColumnIxJson::Block
{
    using CharT = typename std::string::value_type;

    explicit Block(size_t starting_capacity)
        : size(0),
        capacity(starting_capacity),
        data_(new CharT[capacity])
    {}

    inline auto GetAvailable() const {
        return capacity - size;
    }

    std::string_view AppendUnsafe(std::string_view str) {
        const auto pos = &data_[size];

        memcpy(pos, str.data(), str.size());
        size += str.size();

        return std::string_view(pos, str.size());
    }

    auto GetCurrentWritePos() {
        return &data_[size];
    }

    std::string_view ConsumeTailAsStringViewUnsafe(size_t len) {
        const auto start = &data_[size];
        size += len;
        return std::string_view(start, len);
    }

    size_t size;
    const size_t capacity;
    std::unique_ptr<CharT[]> data_;
};

ColumnIxJson::ColumnIxJson()
    : Column(Type::CreateIxJson())
{
}

ColumnIxJson::ColumnIxJson(size_t element_count)
    : Column(Type::CreateIxJson())
{
    items_.reserve(element_count);
    // 100 is arbitrary number, assumption that string values are about ~40 bytes long.
    blocks_.reserve(std::max<size_t>(1, element_count / 100));
}

ColumnIxJson::ColumnIxJson(const std::vector<std::string>& data)
    : ColumnIxJson()
{
    items_.reserve(data.size());
    blocks_.emplace_back(ComputeTotalSize(data));

    for (const auto & s : data) {
        AppendUnsafe(s);
    }
};

ColumnIxJson::ColumnIxJson(std::vector<std::string>&& data)
    : ColumnIxJson()
{
    items_.reserve(data.size());

    for (auto&& d : data) {
        append_data_.emplace_back(std::move(d));
        auto& last_data = append_data_.back();
        items_.emplace_back(std::string_view{ last_data.data(),last_data.length() });
    }
}

ColumnIxJson::~ColumnIxJson()
{}

void ColumnIxJson::Append(std::string_view str) {
    if (blocks_.size() == 0 || blocks_.back().GetAvailable() < str.length()) {
        blocks_.emplace_back(std::max(DEFAULT_BLOCK_SIZE, str.size()));
    }

    items_.emplace_back(blocks_.back().AppendUnsafe(str));
}

void ColumnIxJson::Append(const char* str) {
    Append(std::string_view(str, strlen(str)));
}

void ColumnIxJson::Append(std::string&& steal_value) {
    append_data_.emplace_back(std::move(steal_value));
    auto& last_data = append_data_.back();
    items_.emplace_back(std::string_view{ last_data.data(),last_data.length() });
}

void ColumnIxJson::AppendNoManagedLifetime(std::string_view str) {
    items_.emplace_back(str);
}

void ColumnIxJson::AppendUnsafe(std::string_view str) {
    items_.emplace_back(blocks_.back().AppendUnsafe(str));
}

void ColumnIxJson::Clear() {
    items_.clear();
    blocks_.clear();
    append_data_.clear();
    append_data_.shrink_to_fit();
}

std::string_view ColumnIxJson::At(size_t n) const {
    return items_.at(n);
}

std::string_view ColumnIxJson::operator [] (size_t n) const {
    return items_[n];
}

void ColumnIxJson::Append(ColumnRef column) {
    if (auto col = column->As<ColumnIxJson>()) {
        const auto total_size = ComputeTotalSize(col->items_);

        // TODO: fill up existing block with some items and then add a new one for the rest of items
        if (blocks_.size() == 0 || blocks_.back().GetAvailable() < total_size)
            blocks_.emplace_back(std::max(DEFAULT_BLOCK_SIZE, total_size));

        // Intentionally not doing items_.reserve() since that cripples performance.
        for (size_t i = 0; i < column->Size(); ++i) {
            this->AppendUnsafe((*col)[i]);
        }
    }
}

bool ColumnIxJson::LoadBody(InputStream* input, size_t rows) {
    items_.clear();
    blocks_.clear();

    items_.reserve(rows);
    Block * block = nullptr;

    // TODO(performance): unroll a loop to a first row (to get rid of `blocks_.size() == 0` check) and the rest.
    for (size_t i = 0; i < rows; ++i) {
        uint64_t len;
        if (!WireFormat::ReadUInt64(*input, &len))
            return false;

        if (blocks_.size() == 0 || len > block->GetAvailable())
            block = &blocks_.emplace_back(std::max<size_t>(DEFAULT_BLOCK_SIZE, len));

        if (!WireFormat::ReadBytes(*input, block->GetCurrentWritePos(), len))
            return false;

        items_.emplace_back(block->ConsumeTailAsStringViewUnsafe(len));
    }

    return true;
}

void ColumnIxJson::SaveBody(OutputStream* output) {
    for (const auto & item : items_) {
        WireFormat::WriteObject(*output, item);
    }
}

size_t ColumnIxJson::Size() const {
    return items_.size();
}

ColumnRef ColumnIxJson::Slice(size_t begin, size_t len) const {
    auto result = std::make_shared<ColumnIxJson>();

    if (begin < items_.size()) {
        len = std::min(len, items_.size() - begin);
        result->items_.reserve(len);

        result->blocks_.emplace_back(ComputeTotalSize(items_, begin, len));
        for (size_t i = begin; i < begin + len; ++i) {
            result->Append(items_[i]);
        }
    }

    return result;
}

ColumnRef ColumnIxJson::CloneEmpty() const {
    return std::make_shared<ColumnIxJson>();
}

void ColumnIxJson::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnIxJson &>(other);
    items_.swap(col.items_);
    blocks_.swap(col.blocks_);
    append_data_.swap(col.append_data_);
}

ItemView ColumnIxJson::GetItem(size_t index) const {
    return ItemView{Type::IxJson, this->At(index)};
}

}
