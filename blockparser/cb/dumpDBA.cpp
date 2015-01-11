// Dump transactions in a very simple format.


#include <string>
#include <set>
#include <unordered_map>
#include <vector>
#include <iostream>
#include <fstream>
#include <limits>
using namespace std;

#include <google/sparse_hash_map>
#include <google/sparse_hash_set>
using namespace google;

#include <util.h>
#include <common.h>
#include <errlog.h>
#include <callback.h>

class Address
{
public:
    static const int HASH_SIZE = 20; // 160 bits.

private:
    static const char NULL_HASH[HASH_SIZE];

public:
    Address() noexcept : data{} {}
    Address(const Address&) = default;
    Address(Address&&) = default;

    explicit Address(const char* h)
    {
        memcpy(this->data, h == nullptr ? NULL_HASH : h, HASH_SIZE);
    }

    explicit Address(const uint8_t* h)
    {
        memcpy(this->data, h == nullptr ? NULL_HASH : (const char*)h, HASH_SIZE);
    }

    Address(const std::string& str)
    {
        memcpy(this->data, str.size() == 0 || static_cast<int>(str.size()) != HASH_SIZE ? NULL_HASH : str.data(), HASH_SIZE);
    }

    ~Address() {}

    Address& operator=(const Address&) = default;
    Address& operator=(Address&&) = default;

    /**
      * Return a pointer to its internal data.
      * The length of the returned value is exactly HASH_SIZE.
      */
    inline const char* getData() const noexcept { return this->data; }

    bool isNull() const noexcept
    {
        return memcmp(this->data, NULL_HASH, HASH_SIZE) == 0;
    }

    void writeAsBinaryTo(ostream& stream)
    {
        stream.write(this->data, Address::HASH_SIZE);
    }

private:
    friend ostream& operator<<(ostream& stream, const Address& address);
    friend bool operator==(const Address& h1, const Address& h2);

    char data[HASH_SIZE];
};

namespace std { namespace tr1 {
    /**
     * Needed by 'google::sparse_hash_map' and 'google::sparse_hash_set'.
     */
    template <>
    struct hash<Address>
    {
        size_t operator()(const Address& a) const
        {
            return *(const size_t*)(a.getData());
        }
    };
}}

/**
 * To write a Bitcoin address as human readable (base58 format).
 */
inline ostream& operator<<(ostream& stream, const Address& address)
{
    uint8_t addressText[64];
    hash160ToAddr(addressText, (const uint8_t*)address.data);
    stream << addressText;
    return stream;
}

inline bool operator==(const Address& h1, const Address& h2)
{
    return memcmp(h1.getData(), h2.getData(), Address::HASH_SIZE) == 0;
}

inline bool operator!=(const Address& h1, const Address& h2)
{
     return !(h1 == h2);
}

inline bool operator<(const Address& h1, const Address& h2)
{
    return memcmp(h1.getData(), h2.getData(), Address::HASH_SIZE) < 0;
}

/**
 * Read the output address from an output script.
 */
bool getPayToAddr(uint8_t* addrOutput, const uint8_t* outputScript, uint64_t outputScriptSize)
{
    uint8_t type[2]; // Unused.
    int r = solveOutputScript(addrOutput, outputScript, outputScriptSize, type);
    return r >= 0;
}

struct DumpDBA : public Callback
{
    optparse::OptionParser parser;

    string outputFilename;

    using AddressSet = sparse_hash_set<Address>;
    using Transactions = sparse_hash_map<Address, AddressSet>;
    Transactions transactionsProcessed; // One address to another.
    uint64_t nbTransactionProcessed; // Number of Bitcoin transaction (many-to-many).

    vector<Address> currentInputs; // Inputs for the current transaction.
    vector<Address> currentOutputs; // Outputs for the current transaction.

    bool isGenTX; // Is the transaction generates new coins? (In this case it will be dropped).

    uint64_t heightMax = numeric_limits<uint64_t>::max(); // Parse the whole blockchain by default;
    uint64_t currBlock; // Height of the current block.

    ofstream fileOutput;

    DumpDBA() :
        transactionsProcessed(5000000),
        nbTransactionProcessed(0)
    {
        this->parser
            .usage("[list of transaction hashes]")
            .version("")
            .description(
                "dump the transactions for the DBA project"
            )
            .epilog("")
        ;
        this->parser
            .add_option("-o", "--output")
            .type("string")
            .set_default("output.bin")
            .help("Define the output (default: 'output.bin')")
        ;
        this->parser
            .add_option("-h", "--height")
            .type("uint64_t")
            .set_default(0)
            .help("Define the maximum height (default: unlimited)")
        ;
    }

    const char* name() const override { return "dumpDBA"; }
    const optparse::OptionParser* optionParser() const override { return &parser; }
    bool needTXHash() const override { return true; }

    void aliases(std::vector<const char*> &v) const override
    {
        // No aliases.
    }

    int init(int argc, const char *argv[]) override
    {
        optparse::Values& values = parser.parse_args(argc, argv);

        if (values.is_set("output"))
            this->outputFilename = values["output"];

        if (values.is_set("height") && (uint64_t)values.get("height") != 0)
            this->heightMax = values.get("height");

        cout << "dumbDBA: output: " << this->outputFilename << endl;
        cout << "dumbDBA: height limit: " << this->heightMax << endl;

        this->fileOutput.open(this->outputFilename);

        return 0;
    }

    void startBlock(const Block *b, uint64_t) override
    {
        this->currBlock = b->height;
    }

    void startTX(const uint8_t *p, const uint8_t *hash) override
    {
        this->nbTransactionProcessed += 1;
    }

    void startInput(const uint8_t *p) override
    {
        static uint256_t gNullHash;
        LOAD(uint256_t, upTXHash, p);

        this->isGenTX = (0 == memcmp(gNullHash.v, upTXHash.v, sizeof(gNullHash)));
    }

    /**
     * Input transaction.
     */
    void edge(
        uint64_t      value,
        const uint8_t *upTXHash,
        uint64_t      outputIndex,
        const uint8_t *outputScript,
        uint64_t      outputScriptSize,
        const uint8_t *downTXHash,
        uint64_t      inputIndex,
        const uint8_t *inputScript,
        uint64_t      inputScriptSize
    ) override
    {
        uint8_t address[20];
        if (!this->isGenTX && getPayToAddr(address, outputScript, outputScriptSize))
        {
            Address a(address);
            this->currentInputs.push_back(a);
        }
    }

    /**
     * Output transaction.
     */
    void endOutput(
        const uint8_t *p,                   // Pointer to TX output raw data
        uint64_t      value,                // Number of satoshis on this output
        const uint8_t *txHash,              // sha256 of the current transaction
        uint64_t      outputIndex,          // Index of this output in the current transaction
        const uint8_t *outputScript,        // Raw script (challenge to would-be spender) carried by this output
        uint64_t      outputScriptSize      // Byte size of raw script
    ) override
    {
        uint8_t address[20];
        if (!this->isGenTX && getPayToAddr(address, outputScript, outputScriptSize))
        {
            Address a(address);
            //cout << "Address output: " << a << endl;
            this->currentOutputs.push_back(a);
        }
    }

    void endTX(const uint8_t *p) override
    {
        if (this->currentInputs.size() > 0 && this->currentOutputs.size())
        {
            // For debug.
            // cout << "-----" << endl;
            for (auto input = this->currentInputs.begin(); input != this->currentInputs.end(); ++input)
            {
                AddressSet* currentDestinations = nullptr;
                auto existingInput = this->transactionsProcessed.find(*input);
                if (existingInput == this->transactionsProcessed.end())
                {
                    auto item = this->transactionsProcessed.insert(make_pair(*input, AddressSet()));
                    currentDestinations = &item.first->second;
                }
                else
                    currentDestinations = &existingInput->second;

                for (auto output = this->currentOutputs.begin(); output != this->currentOutputs.end(); ++output)
                {
                    if (*input == *output)
                    {
                        // For debug.
                        // cout << (*input) << " -> " << (*output) << endl;
                        continue;
                    }

                    // The transaction doesn't exist -> we write it to the output.
                    if (currentDestinations->find(*output) == currentDestinations->end())
                    {
                        currentDestinations->insert(*output);
                        input->writeAsBinaryTo(this->fileOutput);
                        output->writeAsBinaryTo(this->fileOutput);
                    }
                }
            }
        }

        this->currentInputs.clear();
        this->currentOutputs.clear();
    }

    void endBlock(const Block* b) override
    {
        if (this->currBlock % 100 == 0)
            cout << "Current height: " << this->currBlock << ", nb transaction: " << this->nbTransactionProcessed << " ..." << endl;

        if (this->currBlock >= this->heightMax)
            exit(0);
    }
};

static DumpDBA dumpDBA;
