// Copyright 2015 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "util/asio.h"
#include "bucket/BucketApplicator.h"
#include "bucket/Bucket.h"
#include "ledger/LedgerDelta.h"
#include "util/Logging.h"

#include <chrono>

namespace stellar
{

BucketApplicator::BucketApplicator(Database& db,
                                   std::shared_ptr<const Bucket> bucket)
    : mDb(db), mBucketIter(bucket)
{
}

BucketApplicator::operator bool() const
{
    return (bool)mBucketIter;
}

typedef std::chrono::duration<double, std::ratio<1>> second_t;

static int adv_calls = 0;
static int adv_iters = 0;
static second_t adv_cum_time(0);

void
BucketApplicator::advance()
{
    typedef std::chrono::high_resolution_clock clock_t;
    std::chrono::time_point<clock_t> beg = clock_t::now();

    soci::transaction sqlTx(mDb.getSession());
    while (mBucketIter)
    {
        LedgerHeader lh;
        LedgerDelta delta(lh, mDb, false);

        auto const& entry = *mBucketIter;
        if (entry.type() == LIVEENTRY)
        {
            EntryFrame::pointer ep = EntryFrame::FromXDR(entry.liveEntry());
            ep->storeAddOrChange(delta, mDb);
        }
        else
        {
            EntryFrame::storeDelete(delta, mDb, entry.deadEntry());
        }
        ++mBucketIter;
        // No-op, just to avoid needless rollback.
        delta.commit();
        if ((++mSize & 0xff) == 0xff)
        {
            break;
        }
        ++adv_iters;
    }
    sqlTx.commit();

    std::chrono::time_point<clock_t> end = clock_t::now();

    ++adv_calls;
    adv_cum_time += end - beg;

    CLOG(INFO, "Bucket") << "* " << adv_calls
                         << " call(s) to BucketApplicator::advance, "
                         << adv_iters << " iteration(s), cumulative time "
                         << adv_cum_time.count() << " second(s)";

    mDb.clearPreparedStatementCache();

    if (!mBucketIter || (mSize & 0xfff) == 0xfff)
    {
        CLOG(INFO, "Bucket")
            << "Bucket-apply: committed " << mSize << " entries";
    }
}
}
