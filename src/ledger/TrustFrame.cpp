// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "ledger/TrustFrame.h"
#include "LedgerDelta.h"
#include "crypto/KeyUtils.h"
#include "crypto/SHA.h"
#include "crypto/SecretKey.h"
#include "database/Database.h"
#include "ledger/LedgerManager.h"
#include "ledger/LedgerRange.h"
#include "util/XDROperators.h"
#include "util/types.h"

using namespace std;
using namespace soci;

namespace stellar
{
// note: the primary key omits assettype as assetcodes are non overlapping
const char* TrustFrame::kSQLCreateStatement1 =
    "CREATE TABLE trustlines"
    "("
    "accountid    VARCHAR(56)     NOT NULL,"
    "assettype    INT             NOT NULL,"
    "issuer       VARCHAR(56)     NOT NULL,"
    "assetcode    VARCHAR(12)     NOT NULL,"
    "tlimit       BIGINT          NOT NULL CHECK (tlimit > 0),"
    "balance      BIGINT          NOT NULL CHECK (balance >= 0),"
    "flags        INT             NOT NULL,"
    "lastmodified INT             NOT NULL,"
    "PRIMARY KEY  (accountid, issuer, assetcode)"
    ");";

TrustFrame::TrustFrame()
    : EntryFrame(TRUSTLINE)
    , mTrustLine(mEntry.data.trustLine())
    , mIsIssuer(false)
{
}

TrustFrame::TrustFrame(LedgerEntry const& from)
    : EntryFrame(from), mTrustLine(mEntry.data.trustLine()), mIsIssuer(false)
{
}

TrustFrame::TrustFrame(TrustFrame const& from) : TrustFrame(from.mEntry)
{
}

TrustFrame&
TrustFrame::operator=(TrustFrame const& other)
{
    if (&other != this)
    {
        mTrustLine = other.mTrustLine;
        mKey = other.mKey;
        mKeyCalculated = other.mKeyCalculated;
        mIsIssuer = other.mIsIssuer;
    }
    return *this;
}

void
TrustFrame::getKeyFields(LedgerKey const& key, std::string& actIDStrKey,
                         std::string& issuerStrKey, std::string& assetCode)
{
    actIDStrKey = KeyUtils::toStrKey(key.trustLine().accountID);
    if (key.trustLine().asset.type() == ASSET_TYPE_CREDIT_ALPHANUM4)
    {
        issuerStrKey =
            KeyUtils::toStrKey(key.trustLine().asset.alphaNum4().issuer);
        assetCodeToStr(key.trustLine().asset.alphaNum4().assetCode, assetCode);
    }
    else if (key.trustLine().asset.type() == ASSET_TYPE_CREDIT_ALPHANUM12)
    {
        issuerStrKey =
            KeyUtils::toStrKey(key.trustLine().asset.alphaNum12().issuer);
        assetCodeToStr(key.trustLine().asset.alphaNum12().assetCode, assetCode);
    }

    if (actIDStrKey == issuerStrKey)
        throw std::runtime_error("Issuer's own trustline should not be used "
                                 "outside of OperationFrame");
}

int64_t
TrustFrame::getBalance() const
{
    return mTrustLine.balance;
}

int64_t
TrustFrame::getAvailableBalance(LedgerManager const& lm) const
{
    int64_t availableBalance = getBalance();
    if (lm.getCurrentLedgerVersion() >= 10)
    {
        availableBalance -= getSellingLiabilities(lm);
    }
    return availableBalance;
}

int64_t
TrustFrame::getMinimumLimit(LedgerManager const& lm) const
{
    int64_t minLimit = getBalance();
    if (lm.getCurrentLedgerVersion() >= 10)
    {
        minLimit += getBuyingLiabilities(lm);
    }
    return minLimit;
}

int64_t
getBuyingLiabilities(TrustLineEntry const& tl, LedgerManager const& lm)
{
    assert(lm.getCurrentLedgerVersion() >= 10);
    return (tl.ext.v() == 0) ? 0 : tl.ext.v1().liabilities.buying;
}

int64_t
getSellingLiabilities(TrustLineEntry const& tl, LedgerManager const& lm)
{
    assert(lm.getCurrentLedgerVersion() >= 10);
    return (tl.ext.v() == 0) ? 0 : tl.ext.v1().liabilities.selling;
}

int64_t
TrustFrame::getBuyingLiabilities(LedgerManager const& lm) const
{
    return stellar::getBuyingLiabilities(mTrustLine, lm);
}

int64_t
TrustFrame::getSellingLiabilities(LedgerManager const& lm) const
{
    return stellar::getSellingLiabilities(mTrustLine, lm);
}

bool
TrustFrame::addBuyingLiabilities(int64_t delta, LedgerManager const& lm)
{
    assert(lm.getCurrentLedgerVersion() >= 10);
    assert(getBalance() >= 0);
    assert(mTrustLine.limit >= 0);
    if (mIsIssuer || delta == 0)
    {
        return true;
    }
    if (!isAuthorized())
    {
        return false;
    }
    int64_t buyingLiab =
        (mTrustLine.ext.v() == 0) ? 0 : mTrustLine.ext.v1().liabilities.buying;

    int64_t maxLiabilities = mTrustLine.limit - getBalance();
    bool res = stellar::addBalance(buyingLiab, delta, maxLiabilities);
    if (res)
    {
        if (mTrustLine.ext.v() == 0)
        {
            mTrustLine.ext.v(1);
            mTrustLine.ext.v1().liabilities = Liabilities{0, 0};
        }
        mTrustLine.ext.v1().liabilities.buying = buyingLiab;
    }
    return res;
}

bool
TrustFrame::addSellingLiabilities(int64_t delta, LedgerManager const& lm)
{
    assert(lm.getCurrentLedgerVersion() >= 10);
    assert(getBalance() >= 0);
    if (mIsIssuer || delta == 0)
    {
        return true;
    }
    if (!isAuthorized())
    {
        return false;
    }
    int64_t sellingLiab =
        (mTrustLine.ext.v() == 0) ? 0 : mTrustLine.ext.v1().liabilities.selling;

    int64_t maxLiabilities = mTrustLine.balance;
    bool res = stellar::addBalance(sellingLiab, delta, maxLiabilities);
    if (res)
    {
        if (mTrustLine.ext.v() == 0)
        {
            mTrustLine.ext.v(1);
            mTrustLine.ext.v1().liabilities = Liabilities{0, 0};
        }
        mTrustLine.ext.v1().liabilities.selling = sellingLiab;
    }
    return res;
}

bool
TrustFrame::isAuthorized() const
{
    return (mTrustLine.flags & AUTHORIZED_FLAG) != 0;
}

void
TrustFrame::setAuthorized(bool authorized)
{
    if (authorized)
    {
        mTrustLine.flags |= AUTHORIZED_FLAG;
    }
    else
    {
        mTrustLine.flags &= ~AUTHORIZED_FLAG;
    }
}

bool
TrustFrame::addBalance(int64_t delta, LedgerManager const& lm)
{
    if (mIsIssuer || delta == 0)
    {
        return true;
    }
    if (!isAuthorized())
    {
        return false;
    }

    auto newBalance = mTrustLine.balance;
    if (!stellar::addBalance(newBalance, delta, mTrustLine.limit))
    {
        return false;
    }
    if (lm.getCurrentLedgerVersion() >= 10)
    {
        if (newBalance < getSellingLiabilities(lm))
        {
            return false;
        }
        if (newBalance > mTrustLine.limit - getBuyingLiabilities(lm))
        {
            return false;
        }
    }

    mTrustLine.balance = newBalance;
    return true;
}

int64_t
TrustFrame::getMaxAmountReceive(LedgerManager& lm) const
{
    int64_t amount = 0;
    if (mIsIssuer)
    {
        amount = INT64_MAX;
    }
    else if (isAuthorized())
    {
        amount = mTrustLine.limit - mTrustLine.balance;
        if (lm.getCurrentLedgerVersion() >= 10)
        {
            amount -= getBuyingLiabilities(lm);
        }
    }
    return amount;
}

bool
TrustFrame::exists(Database& db, LedgerKey const& key)
{
    if (cachedEntryExists(key, db) && getCachedEntry(key, db) != nullptr)
    {
        return true;
    }

    std::string actIDStrKey, issuerStrKey, assetCode;
    getKeyFields(key, actIDStrKey, issuerStrKey, assetCode);
    int exists = 0;
    auto timer = db.getSelectTimer("trust-exists");
    auto prep = db.getPreparedStatement(
        "SELECT EXISTS (SELECT NULL FROM trustlines "
        "WHERE accountid=:v1 AND issuer=:v2 AND assetcode=:v3)");
    auto& st = prep.statement();
    st.exchange(use(actIDStrKey));
    st.exchange(use(issuerStrKey));
    st.exchange(use(assetCode));
    st.exchange(into(exists));
    st.define_and_bind();
    st.execute(true);
    return exists != 0;
}

uint64_t
TrustFrame::countObjects(soci::session& sess)
{
    uint64_t count = 0;
    sess << "SELECT COUNT(*) FROM trustlines;", into(count);
    return count;
}

uint64_t
TrustFrame::countObjects(soci::session& sess, LedgerRange const& ledgers)
{
    uint64_t count = 0;
    sess << "SELECT COUNT(*) FROM trustlines"
            " WHERE lastmodified >= :v1 AND lastmodified <= :v2;",
        into(count), use(ledgers.first()), use(ledgers.last());
    return count;
}

void
TrustFrame::deleteTrustLinesModifiedOnOrAfterLedger(Database& db,
                                                    uint32_t oldestLedger)
{
    db.getEntryCache().erase_if(
        [oldestLedger](std::shared_ptr<LedgerEntry const> le) -> bool {
            return le && le->data.type() == TRUSTLINE &&
                   le->lastModifiedLedgerSeq >= oldestLedger;
        });

    {
        auto prep = db.getPreparedStatement(
            "DELETE FROM trustlines WHERE lastmodified >= :v1");
        auto& st = prep.statement();
        st.exchange(soci::use(oldestLedger));
        st.define_and_bind();
        st.execute(true);
    }
}

void
TrustFrame::storeDelete(LedgerDelta& delta, Database& db) const
{
    storeDelete(delta, db, getKey());
}

void
TrustFrame::storeDelete(LedgerDelta& delta, Database& db, LedgerKey const& key)
{
    flushCachedEntry(key, db);

    std::string actIDStrKey, issuerStrKey, assetCode;
    getKeyFields(key, actIDStrKey, issuerStrKey, assetCode);

    auto timer = db.getDeleteTimer("trust");
    db.getSession() << "DELETE FROM trustlines "
                       "WHERE accountid=:v1 AND issuer=:v2 AND assetcode=:v3",
        use(actIDStrKey), use(issuerStrKey), use(assetCode);

    delta.deleteEntry(key);
}

void
TrustFrame::storeAddOrChange(LedgerDelta& delta, Database& db, int mode,
                             bool bulk)
{
    auto key = getKey();
    flushCachedEntry(key, db);

    if (mIsIssuer)
    {
        return;
    }

    touch(delta);

    string actIDStrKey, issuerStrKey, assetCode;
    getKeyFields(key, actIDStrKey, issuerStrKey, assetCode);

    Liabilities liabilities;
    soci::indicator liabilitiesInd = soci::i_null;
    if (mTrustLine.ext.v() == 1)
    {
        liabilities = mTrustLine.ext.v1().liabilities;
        liabilitiesInd = soci::i_ok;
    }

    string sql;
    bool insert = false;

    bool pg = false;
    if (mode == 0)
    {
        pg = true || db.isPG();
    }

    if (pg)
    {
        string table = "trustlines";
        if (bulk)
        {
            table += "_bulk";
        }
        sql = ("INSERT INTO " + table +
               " "
               "(accountid, assettype, issuer, assetcode, balance, tlimit, "
               "flags, lastmodified, buyingliabilities, sellingliabilities) "
               "VALUES (:accountid, :assettype, :issuer, :assetcode, :b, :tl, "
               ":a, :lm, :bl, :sl) "
               "ON CONFLICT (accountid, issuer, assetcode) DO UPDATE "
               "SET balance=:b, tlimit=:tl, flags=:a, lastmodified=:lm, "
               "buyingliabilities=:bl, sellingliabilities=:sl "
               "RETURNING xmax");
    }
    else if (mode == 2 || (mode == 0 && exists(db, key)))
    {
        insert = false;
        sql = ("UPDATE trustlines "
               "SET balance=:b, tlimit=:tl, flags=:a, lastmodified=:lm, "
               "buyingliabilities=:bl, sellingliabilities=:sl "
               "WHERE accountid=:accountid AND issuer=:issuer AND "
               "assetcode=:assetcode");
    }
    else
    {
        insert = true;
        sql = ("INSERT INTO trustlines "
               "(accountid, assettype, issuer, assetcode, balance, tlimit, "
               "flags, lastmodified, buyingliabilities, sellingliabilities) "
               "VALUES (:accountid, :assettype, :issuer, :assetcode, :b, :tl, "
               ":a, :lm, :bl, :sl)");
    }
    auto prep = db.getPreparedStatement(sql);
    auto& st = prep.statement();
    st.exchange(use(actIDStrKey, "accountid"));
    st.exchange(use(issuerStrKey, "issuer"));
    st.exchange(use(assetCode, "assetcode"));
    st.exchange(use(mTrustLine.balance, "b"));
    st.exchange(use(mTrustLine.limit, "tl"));
    st.exchange(use(mTrustLine.flags, "a"));
    st.exchange(use(getLastModified(), "lm"));
    st.exchange(use(liabilities.buying, liabilitiesInd, "bl"));
    st.exchange(use(liabilities.selling, liabilitiesInd, "sl"));
    if (pg || insert)
    {
        unsigned int assetType = key.trustLine().asset.type();
        st.exchange(use(assetType, "assettype"));
    }
    int xmax;
    if (pg)
    {
        st.exchange(into(xmax));
    }
    st.define_and_bind();
    if (insert)
    {
        auto timer = db.getInsertTimer("trust");
        st.execute(true);
    }
    else
    {
        auto timer = db.getUpdateTimer("trust");
        st.execute(true);
    }
    if (st.get_affected_rows() != 1)
    {
        throw std::runtime_error("Could not update data in SQL");
    }
    if (pg)
    {
        insert = xmax == 0;
    }
    if (insert)
    {
        delta.addEntry(*this);
    }
    else
    {
        delta.modEntry(*this);
    }
}

void
TrustFrame::mergeBulkTable(soci::session& sess)
{
    sess << "UPDATE trustlines "
         << "SET balance = b.balance, tlimit = b.tlimit, flags = b.flags, "
            "lastmodified = b.lastmodified, buyingliabilities = "
            "b.buyingliabilities, sellingliabilities = b.sellingliabilities "
         << "FROM trustlines_bulk b "
         << "WHERE trustlines.accountid = b.accountid AND trustlines.issuer = "
            "b.issuer AND trustlines.assetcode = b.assetcode";

    sess << "INSERT INTO trustlines "
         << "SELECT * FROM trustlines_bulk "
         << "ON CONFLICT (accountid, issuer, assetcode) DO NOTHING";
}

static const char* trustLineColumnSelector =
    "SELECT "
    "accountid,assettype,issuer,assetcode,tlimit,balance,flags,lastmodified,"
    "buyingliabilities,sellingliabilities "
    "FROM trustlines";

TrustFrame::pointer
TrustFrame::createIssuerFrame(Asset const& issuer)
{
    pointer res = make_shared<TrustFrame>();
    res->mIsIssuer = true;
    TrustLineEntry& tl = res->mEntry.data.trustLine();

    tl.accountID = getIssuer(issuer);

    tl.flags |= AUTHORIZED_FLAG;
    tl.balance = INT64_MAX;
    tl.asset = issuer;
    tl.limit = INT64_MAX;
    return res;
}

TrustFrame::pointer
TrustFrame::loadTrustLine(AccountID const& accountID, Asset const& asset,
                          Database& db, LedgerDelta* delta)
{
    if (asset.type() == ASSET_TYPE_NATIVE)
    {
        throw std::runtime_error("XLM TrustLine?");
    }
    else
    {
        if (accountID == getIssuer(asset))
        {
            return createIssuerFrame(asset);
        }
    }

    LedgerKey key;
    key.type(TRUSTLINE);
    key.trustLine().accountID = accountID;
    key.trustLine().asset = asset;
    if (cachedEntryExists(key, db))
    {
        auto p = getCachedEntry(key, db);
        if (p)
        {
            pointer ret = std::make_shared<TrustFrame>(*p);
            if (delta)
            {
                delta->recordEntry(*ret);
            }
            return ret;
        }
    }

    std::string accStr, issuerStr, assetStr;

    accStr = KeyUtils::toStrKey(accountID);
    if (asset.type() == ASSET_TYPE_CREDIT_ALPHANUM4)
    {
        assetCodeToStr(asset.alphaNum4().assetCode, assetStr);
        issuerStr = KeyUtils::toStrKey(asset.alphaNum4().issuer);
    }
    else if (asset.type() == ASSET_TYPE_CREDIT_ALPHANUM12)
    {
        assetCodeToStr(asset.alphaNum12().assetCode, assetStr);
        issuerStr = KeyUtils::toStrKey(asset.alphaNum12().issuer);
    }

    auto query = std::string(trustLineColumnSelector);
    query += (" WHERE accountid = :id "
              " AND issuer = :issuer "
              " AND assetcode = :asset");
    auto prep = db.getPreparedStatement(query);
    auto& st = prep.statement();
    st.exchange(use(accStr));
    st.exchange(use(issuerStr));
    st.exchange(use(assetStr));

    pointer retLine;
    auto timer = db.getSelectTimer("trust");
    loadLines(prep, [&retLine](LedgerEntry const& trust) {
        retLine = make_shared<TrustFrame>(trust);
    });

    if (retLine)
    {
        retLine->putCachedEntry(db);
    }
    else
    {
        putCachedEntry(key, nullptr, db);
    }

    if (delta && retLine)
    {
        delta->recordEntry(*retLine);
    }
    return retLine;
}

std::pair<TrustFrame::pointer, AccountFrame::pointer>
TrustFrame::loadTrustLineIssuer(AccountID const& accountID, Asset const& asset,
                                Database& db, LedgerDelta& delta)
{
    std::pair<TrustFrame::pointer, AccountFrame::pointer> res;

    res.first = loadTrustLine(accountID, asset, db, &delta);
    res.second = AccountFrame::loadAccount(delta, getIssuer(asset), db);
    return res;
}

void
TrustFrame::loadLines(StatementContext& prep,
                      std::function<void(LedgerEntry const&)> trustProcessor)
{
    string actIDStrKey;
    std::string issuerStrKey, assetCode;
    unsigned int assetType;

    Liabilities liabilities;
    soci::indicator buyingLiabilitiesInd;
    soci::indicator sellingLiabilitiesInd;

    LedgerEntry le;
    le.data.type(TRUSTLINE);

    TrustLineEntry& tl = le.data.trustLine();

    auto& st = prep.statement();
    st.exchange(into(actIDStrKey));
    st.exchange(into(assetType));
    st.exchange(into(issuerStrKey));
    st.exchange(into(assetCode));
    st.exchange(into(tl.limit));
    st.exchange(into(tl.balance));
    st.exchange(into(tl.flags));
    st.exchange(into(le.lastModifiedLedgerSeq));
    st.exchange(into(liabilities.buying, buyingLiabilitiesInd));
    st.exchange(into(liabilities.selling, sellingLiabilitiesInd));
    st.define_and_bind();

    st.execute(true);
    while (st.got_data())
    {
        tl.accountID = KeyUtils::fromStrKey<PublicKey>(actIDStrKey);
        tl.asset.type((AssetType)assetType);
        if (assetType == ASSET_TYPE_CREDIT_ALPHANUM4)
        {
            tl.asset.alphaNum4().issuer =
                KeyUtils::fromStrKey<PublicKey>(issuerStrKey);
            strToAssetCode(tl.asset.alphaNum4().assetCode, assetCode);
        }
        else if (assetType == ASSET_TYPE_CREDIT_ALPHANUM12)
        {
            tl.asset.alphaNum12().issuer =
                KeyUtils::fromStrKey<PublicKey>(issuerStrKey);
            strToAssetCode(tl.asset.alphaNum12().assetCode, assetCode);
        }

        assert(buyingLiabilitiesInd == sellingLiabilitiesInd);
        if (buyingLiabilitiesInd == soci::i_ok)
        {
            tl.ext.v(1);
            tl.ext.v1().liabilities = liabilities;
        }

        trustProcessor(le);

        st.fetch();
    }
}

void
TrustFrame::loadLines(AccountID const& accountID,
                      std::vector<TrustFrame::pointer>& retLines, Database& db)
{
    std::string actIDStrKey;
    actIDStrKey = KeyUtils::toStrKey(accountID);

    auto query = std::string(trustLineColumnSelector);
    query += (" WHERE accountid = :id ");
    auto prep = db.getPreparedStatement(query);
    auto& st = prep.statement();
    st.exchange(use(actIDStrKey));

    auto timer = db.getSelectTimer("trust");
    loadLines(prep, [&retLines](LedgerEntry const& cur) {
        retLines.emplace_back(make_shared<TrustFrame>(cur));
    });
}

std::unordered_map<AccountID, std::vector<TrustFrame::pointer>>
TrustFrame::loadAllLines(Database& db)
{
    std::unordered_map<AccountID, std::vector<TrustFrame::pointer>> retLines;

    auto query = std::string(trustLineColumnSelector);
    query += (" ORDER BY accountid");
    auto prep = db.getPreparedStatement(query);

    auto timer = db.getSelectTimer("trust");
    loadLines(prep, [&retLines](LedgerEntry const& cur) {
        auto& thisUserLines = retLines[cur.data.trustLine().accountID];
        thisUserLines.emplace_back(make_shared<TrustFrame>(cur));
    });
    return retLines;
}

void
TrustFrame::dropAll(Database& db)
{
    db.getSession() << "DROP TABLE IF EXISTS trustlines;";
    db.getSession() << kSQLCreateStatement1;
}
}
