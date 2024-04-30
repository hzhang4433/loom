#pragma once

class minWRollback
{
    public:
        minWRollback() = default;

        ~minWRollback() = default;

        void execute();

        void buileGraph();

        void rollback();

    private:
        //some type => rollbackTxs;
};