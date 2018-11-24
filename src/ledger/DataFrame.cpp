// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "ledger/DataFrame.h"
#include "LedgerDelta.h"
#include "crypto/KeyUtils.h"
#include "crypto/SHA.h"
#include "crypto/SecretKey.h"
#include "database/Database.h"
#include "ledger/LedgerRange.h"
#include "transactions/ManageDataOpFrame.h"
#include "util/Decoder.h"
#include "util/types.h"

using namespace std;
using namespace soci;

namespace stellar
{
const char* DataFrame::kSQLCreateStatement1 =
    "CREATE TABLE accountdata"
    "("
    "accountid    VARCHAR(56)  NOT NULL,"
    "dataname     VARCHAR(64)  NOT NULL,"
    "datavalue    VARCHAR(112) NOT NULL,"
    "PRIMARY KEY  (accountid, dataname)"
    ");";

static const char* dataColumnSelector =
    "SELECT accountid,dataname,datavalue,lastmodified FROM accountdata";

DataFrame::DataFrame() : EntryFrame(DATA), mData(mEntry.data.data())
{
}

DataFrame::DataFrame(LedgerEntry const& from)
    : EntryFrame(from), mData(mEntry.data.data())
{
}

DataFrame::DataFrame(DataFrame const& from) : DataFrame(from.mEntry)
{
}

DataFrame&
DataFrame::operator=(DataFrame const& other)
{
    if (&other != this)
    {
        mData = other.mData;
        mKey = other.mKey;
        mKeyCalculated = other.mKeyCalculated;
    }
    return *this;
}

std::string const&
DataFrame::getName() const
{
    return mData.dataName;
}

stellar::DataValue const&
DataFrame::getValue() const
{
    return mData.dataValue;
}

AccountID const&
DataFrame::getAccountID() const
{
    return mData.accountID;
}

DataFrame::pointer
DataFrame::loadData(AccountID const& accountID, std::string dataName,
                    Database& db)
{
    DataFrame::pointer retData;

    std::string actIDStrKey = KeyUtils::toStrKey(accountID);

    std::string sql = dataColumnSelector;
    sql += " WHERE accountid = :id AND dataname = :dataname";
    auto prep = db.getPreparedStatement(sql);
    auto& st = prep.statement();
    st.exchange(use(actIDStrKey));
    st.exchange(use(dataName));

    auto timer = db.getSelectTimer("data");
    loadData(prep, [&retData](LedgerEntry const& data) {
        retData = make_shared<DataFrame>(data);
    });

    return retData;
}

void
DataFrame::loadData(StatementContext& prep,
                    std::function<void(LedgerEntry const&)> dataProcessor)
{
    string actIDStrKey;

    std::string dataName, dataValue;

    soci::indicator dataNameIndicator, dataValueIndicator;

    LedgerEntry le;
    le.data.type(DATA);
    DataEntry& oe = le.data.data();

    statement& st = prep.statement();
    st.exchange(into(actIDStrKey));
    st.exchange(into(dataName, dataNameIndicator));
    st.exchange(into(dataValue, dataValueIndicator));
    st.exchange(into(le.lastModifiedLedgerSeq));
    st.define_and_bind();
    st.execute(true);
    while (st.got_data())
    {
        oe.accountID = KeyUtils::fromStrKey<PublicKey>(actIDStrKey);

        if ((dataNameIndicator != soci::i_ok) ||
            (dataValueIndicator != soci::i_ok))
        {
            throw std::runtime_error("bad database state");
        }
        oe.dataName = dataName;
        decoder::decode_b64(dataValue, oe.dataValue);

        dataProcessor(le);
        st.fetch();
    }
}

std::unordered_map<AccountID, std::vector<DataFrame::pointer>>
DataFrame::loadAllData(Database& db)
{
    std::unordered_map<AccountID, std::vector<DataFrame::pointer>> retData;
    std::string sql = dataColumnSelector;
    sql += " ORDER BY accountid";
    auto prep = db.getPreparedStatement(sql);

    auto timer = db.getSelectTimer("data");
    loadData(prep, [&retData](LedgerEntry const& of) {
        auto& thisUserData = retData[of.data.data().accountID];
        thisUserData.emplace_back(make_shared<DataFrame>(of));
    });
    return retData;
}

bool
DataFrame::exists(Database& db, LedgerKey const& key)
{
    std::string actIDStrKey = KeyUtils::toStrKey(key.data().accountID);
    std::string dataName = key.data().dataName;
    int exists = 0;
    auto timer = db.getSelectTimer("data-exists");
    auto prep =
        db.getPreparedStatement("SELECT EXISTS (SELECT NULL FROM accountdata "
                                "WHERE accountid=:id AND dataname=:s)");
    auto& st = prep.statement();
    st.exchange(use(actIDStrKey));
    st.exchange(use(dataName));
    st.exchange(into(exists));
    st.define_and_bind();
    st.execute(true);
    return exists != 0;
}

uint64_t
DataFrame::countObjects(soci::session& sess)
{
    uint64_t count = 0;
    sess << "SELECT COUNT(*) FROM accountdata;", into(count);
    return count;
}

uint64_t
DataFrame::countObjects(soci::session& sess, LedgerRange const& ledgers)
{
    uint64_t count = 0;
    sess << "SELECT COUNT(*) FROM accountdata"
            " WHERE lastmodified >= :v1 AND lastmodified <= :v2;",
        into(count), use(ledgers.first()), use(ledgers.last());
    return count;
}

void
DataFrame::deleteDataModifiedOnOrAfterLedger(Database& db,
                                             uint32_t oldestLedger)
{
    db.getEntryCache().erase_if(
        [oldestLedger](std::shared_ptr<LedgerEntry const> le) -> bool {
            return le && le->data.type() == DATA &&
                   le->lastModifiedLedgerSeq >= oldestLedger;
        });

    {
        auto prep = db.getPreparedStatement(
            "DELETE FROM accountdata WHERE lastmodified >= :v1");
        auto& st = prep.statement();
        st.exchange(soci::use(oldestLedger));
        st.define_and_bind();
        st.execute(true);
    }
}

void
DataFrame::storeDelete(LedgerDelta& delta, Database& db) const
{
    storeDelete(delta, db, getKey());
}

void
DataFrame::storeDelete(LedgerDelta& delta, Database& db, LedgerKey const& key)
{
    std::string actIDStrKey = KeyUtils::toStrKey(key.data().accountID);
    std::string dataName = key.data().dataName;
    auto timer = db.getDeleteTimer("data");
    auto prep = db.getPreparedStatement(
        "DELETE FROM accountdata WHERE accountid=:id AND dataname=:s");
    auto& st = prep.statement();
    st.exchange(use(actIDStrKey));
    st.exchange(use(dataName));
    st.define_and_bind();
    st.execute(true);
    delta.deleteEntry(key);
}

void
DataFrame::storeAddOrChange(LedgerDelta& delta, Database& db, int mode,
                            bool bulk)
{
    touch(delta);

    std::string actIDStrKey = KeyUtils::toStrKey(mData.accountID);
    std::string dataName = mData.dataName;
    std::string dataValue = decoder::encode_b64(mData.dataValue);

    string sql;
    bool insert = false;

    bool pg = false;
    if (mode == 0)
    {
        pg = true || db.isPG(); // xxx
    }

    if (pg)
    {
        string table = "accountdata";
        if (bulk)
        {
            table += "_bulk";
        }
        sql = ("INSERT INTO " + table +
               " "
               "(accountid, dataname, datavalue, lastmodified) "
               "VALUES (:aid, :dn, :dv, :lm) "
               "ON CONFLICT (accountid, dataname) "
               "DO UPDATE "
               "SET datavalue=:dv, lastmodified=:lm "
               "RETURNING xmax");
    }
    else if (mode == 2 || (mode == 0 && exists(db, getKey())))
    {
        insert = false;
        sql = ("UPDATE accountdata SET datavalue=:dv,lastmodified=:lm "
               " WHERE accountid=:aid AND dataname=:dn");
    }
    else
    {
        insert = true;
        sql = ("INSERT INTO accountdata "
               "(accountid,dataname,datavalue,lastmodified)"
               " VALUES (:aid,:dn,:dv,:lm)");
    }

    auto prep = db.getPreparedStatement(sql);
    auto& st = prep.statement();

    st.exchange(use(actIDStrKey, "aid"));
    st.exchange(use(dataName, "dn"));
    st.exchange(use(dataValue, "dv"));
    st.exchange(use(getLastModified(), "lm"));

    int xmax;
    if (pg)
    {
        st.exchange(into(xmax));
    }

    st.define_and_bind();
    st.execute(true);

    if (st.get_affected_rows() != 1)
    {
        throw std::runtime_error("could not update SQL");
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
DataFrame::mergeAccumulated(soci::session& dest, soci::session& src)
{
  vector<xxx> accountids;
  vector<xxx> datanames;
  vector<xxx> datavalues;
  vector<xxx> lastmodifieds;

  soci::statement st =
    (src.prepare <<
     "SELECT accountid, dataname, datavalue, lastmodified "
     "FROM accountdata",
     into(accountid), into(dataname), into(datavalue), into(lastmodified));
  st.execute(true);

  while (st.got_data()) {
    accountids.push_back(accountid);
    datanames.push_back(dataname);
    datavalues.push_back(datavalue);
    lastmodifieds.push_back(lastmodified);
  }

    sess << "UPDATE accountdata "
         << "SET datavalue = b.datavalue, lastmodified = b.lastmodified "
         << "FROM accountdata_bulk b "
         << "WHERE accountdata.accountid = b.accountid AND "
            "accountdata.dataname = b.dataname";

    sess << "INSERT INTO accountdata "
         << "SELECT * FROM accountdata_bulk "
         << "ON CONFLICT (accountid, dataname) DO NOTHING";
}

void
DataFrame::dropAll(Database& db)
{
    db.getSession() << "DROP TABLE IF EXISTS accountdata;";
    db.getSession() << kSQLCreateStatement1;
}
}
