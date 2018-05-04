/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/utility/utils.h>
#include <dsn/utility/strings.h>
#include <dsn/utility/binary_reader.h>
#include <dsn/utility/binary_writer.h>
#include <dsn/utility/link.h>
#include <dsn/utility/crc.h>
#include <dsn/utility/autoref_ptr.h>
#include <dsn/c/api_layer1.h>
#include <gtest/gtest.h>

using namespace ::dsn;
using namespace ::dsn::utils;

TEST(core, get_last_component)
{
    ASSERT_EQ("a", get_last_component("a", "/"));
    ASSERT_EQ("b", get_last_component("a/b", "/"));
    ASSERT_EQ("b", get_last_component("a//b", "/"));
    ASSERT_EQ("", get_last_component("a/", "/"));
    ASSERT_EQ("c", get_last_component("a/b_c", "/_"));
}

TEST(core, crc)
{
    char buffer[24];
    for (int i = 0; i < sizeof(buffer) / sizeof(char); i++) {
        buffer[i] = dsn_random32(0, 200);
    }

    auto c1 = dsn::utils::crc32_calc(buffer, 12, 0);
    auto c2 = dsn::utils::crc32_calc(buffer + 12, 12, c1);
    auto c3 = dsn::utils::crc32_calc(buffer, 24, 0);
    auto c4 = dsn::utils::crc32_concat(0, 0, c1, 12, c1, c2, 12);
    EXPECT_TRUE(c3 == c4);
}

TEST(core, binary_io)
{
    int value = 0xdeadbeef;
    binary_writer writer;
    writer.write(value);

    auto buf = writer.get_buffer();
    binary_reader reader(buf);
    int value3;
    reader.read(value3);

    EXPECT_TRUE(value3 == value);
}

TEST(core, split_args)
{
    std::string value = "a ,b, c ";
    std::vector<std::string> sargs;
    std::list<std::string> sargs2;
    ::dsn::utils::split_args(value.c_str(), sargs, ',');
    ::dsn::utils::split_args(value.c_str(), sargs2, ',');

    EXPECT_EQ(sargs.size(), 3);
    EXPECT_EQ(sargs[0], "a");
    EXPECT_EQ(sargs[1], "b");
    EXPECT_EQ(sargs[2], "c");

    EXPECT_EQ(sargs2.size(), 3);
    auto it = sargs2.begin();
    EXPECT_EQ(*it++, "a");
    EXPECT_EQ(*it++, "b");
    EXPECT_EQ(*it++, "c");
}

TEST(core, trim_string)
{
    std::string value = " x x x x ";
    auto r = trim_string((char *)value.c_str());
    EXPECT_EQ(std::string(r), "x x x x");
}

TEST(core, dlink)
{
    dlink links[10];
    dlink hdr;

    for (int i = 0; i < 10; i++)
        links[i].insert_before(&hdr);

    int count = 0;
    dlink *p = hdr.next();
    while (p != &hdr) {
        count++;
        p = p->next();
    }

    EXPECT_EQ(count, 10);

    p = hdr.next();
    while (p != &hdr) {
        auto p1 = p;
        p = p->next();
        p1->remove();
        count--;
    }

    EXPECT_TRUE(hdr.is_alone());
    EXPECT_TRUE(count == 0);
}

class foo : public ::dsn::ref_counter
{
public:
    foo(int &count) : _count(count) { _count++; }

    ~foo() { _count--; }

private:
    int &_count;
};

typedef ::dsn::ref_ptr<foo> foo_ptr;

TEST(core, ref_ptr)
{
    int count = 0;
    foo_ptr x = nullptr;
    auto y = new foo(count);
    x = y;
    EXPECT_TRUE(x->get_count() == 1);
    EXPECT_TRUE(count == 1);
    x = new foo(count);
    EXPECT_TRUE(x->get_count() == 1);
    EXPECT_TRUE(count == 1);
    x = nullptr;
    EXPECT_TRUE(count == 0);

    std::map<int, foo_ptr> xs;
    x = new foo(count);
    EXPECT_TRUE(x->get_count() == 1);
    EXPECT_TRUE(count == 1);
    xs.insert(std::make_pair(1, x));
    EXPECT_TRUE(x->get_count() == 2);
    EXPECT_TRUE(count == 1);
    x = nullptr;
    EXPECT_TRUE(count == 1);
    xs.clear();
    EXPECT_TRUE(count == 0);

    x = new foo(count);
    EXPECT_TRUE(count == 1);
    xs[2] = x;
    EXPECT_TRUE(x->get_count() == 2);
    x = nullptr;
    EXPECT_TRUE(count == 1);
    xs.clear();
    EXPECT_TRUE(count == 0);

    y = new foo(count);
    EXPECT_TRUE(count == 1);
    xs.insert(std::make_pair(1, y));
    EXPECT_TRUE(count == 1);
    EXPECT_TRUE(y->get_count() == 1);
    xs.clear();
    EXPECT_TRUE(count == 0);

    y = new foo(count);
    EXPECT_TRUE(count == 1);
    xs[2] = y;
    EXPECT_TRUE(count == 1);
    EXPECT_TRUE(y->get_count() == 1);
    xs.clear();
    EXPECT_TRUE(count == 0);

    foo_ptr z = new foo(count);
    EXPECT_TRUE(count == 1);
    z = std::move(foo_ptr());
    EXPECT_TRUE(count == 0);
}

TEST(utils, time_s_to_date_time) {
    time_t t = time(NULL);
    struct tm lt = {0};
    localtime_r(&t, &lt);

    // std::string time_s_to_date_time(uint64_t ts_s); // yyyy-MM-dd hh:mm:ss
    ASSERT_EQ(time_s_to_date_time(0 - lt.tm_gmtoff), "1970-01-01 00:00:00");
    ASSERT_EQ(time_s_to_date_time(1524833734 - lt.tm_gmtoff), "2018-04-27 12:55:34");
}

TEST(utils, hm_of_day_to_sec) {
    // int32_t hm_of_day_to_sec(const std::string &hm)
    ASSERT_EQ(hm_of_day_to_sec("00:00"), 0);
    ASSERT_EQ(hm_of_day_to_sec("23:59"), 86340);
    ASSERT_EQ(hm_of_day_to_sec("1:1"), 3660);
    ASSERT_EQ(hm_of_day_to_sec("23"), -1);
    ASSERT_EQ(hm_of_day_to_sec("23:"), -1);
    ASSERT_EQ(hm_of_day_to_sec(":59"), -1);
    ASSERT_EQ(hm_of_day_to_sec("-1:00"), -1);
    ASSERT_EQ(hm_of_day_to_sec("24:00"), -1);
    ASSERT_EQ(hm_of_day_to_sec("01:-1"), -1);
    ASSERT_EQ(hm_of_day_to_sec("01:60"), -1);
    ASSERT_EQ(hm_of_day_to_sec("a:00"), -1);
    ASSERT_EQ(hm_of_day_to_sec("01:b"), -1);
    ASSERT_EQ(hm_of_day_to_sec("01b"), -1);
}

TEST(utils, sec_of_day_to_hm) {
    // std::string sec_of_day_to_hm(int32_t sec)
    ASSERT_EQ(sec_of_day_to_hm(0), "00:00");
    ASSERT_EQ(sec_of_day_to_hm(59), "00:00");
    ASSERT_EQ(sec_of_day_to_hm(60), "00:01");
    ASSERT_EQ(sec_of_day_to_hm(86340), "23:59");
    ASSERT_EQ(sec_of_day_to_hm(86399), "23:59");
    ASSERT_EQ(sec_of_day_to_hm(86400), "00:00");
    ASSERT_EQ(sec_of_day_to_hm(3600), "01:00");
}
