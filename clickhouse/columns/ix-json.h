#pragma once

#include "column.h"

#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <deque>

namespace clickhouse {

/**
 * Represents column of variable-length JSON strings.
 */
class ColumnIxJson : public Column {
public:
    // Type this column takes as argument of Append and returns with At() and operator[]
    using ValueType = std::string_view;

    ColumnIxJson();
    ~ColumnIxJson();

    explicit ColumnIxJson(size_t element_count);
    explicit ColumnIxJson(const std::vector<std::string> & data);
    explicit ColumnIxJson(std::vector<std::string>&& data);
    ColumnIxJson& operator=(const ColumnIxJson&) = delete;
    ColumnIxJson(const ColumnIxJson&) = delete;

    /// Appends one element to the column.
    void Append(std::string_view str);

    /// Appends one element to the column.
    void Append(const char* str);

    /// Appends one element to the column.
    void Append(std::string&& steal_value);

    /// Appends one element to the column.
    /// If str lifetime is managed elsewhere and guaranteed to outlive the Block sent to the server
    void AppendNoManagedLifetime(std::string_view str);

    /// Returns element at given row number.
    std::string_view At(size_t n) const;

    /// Returns element at given row number.
    std::string_view operator [] (size_t n) const;

public:
    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output) override;

    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;
    ItemView GetItem(size_t) const override;

private:
    void AppendUnsafe(std::string_view);

private:
    struct Block;

    std::vector<std::string_view> items_;
    std::vector<Block> blocks_;
    std::deque<std::string> append_data_;
};

}
