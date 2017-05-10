/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

    //numeber of frames in the pool = buffs which is passed in
    //creates a buffe rpool of buffs size


    //buff mnger deals with the whole pool

    //BuffDesc class deals with individual frame

    //structs public while classes private


    BufMgr::BufMgr(std::uint32_t bufs) : numBufs(bufs) {

        //creates pool/table one per frame
        //array of BuffDec objects
        bufDescTable = new BufDesc[bufs];

        for (FrameId i = 0; i < numBufs; i++)
        {
            bufDescTable[i].frameNo = i;
            bufDescTable[i].valid = false;
            bufDescTable[i].Clear();
        }

        bufPool = new Page[bufs];

        int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
        hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

        clockHand = bufs - 1;
    }


    //clear all rames
    BufMgr::~BufMgr() {

        //write back all dirty frames. first find all files that need to be written back
        for (FrameId i = 0; i < numBufs; i++) {
            if (bufDescTable[i].valid) {
                if (bufDescTable[i].dirty) {

                    bufDescTable[i].file->writePage(bufPool[i]);
                }
                bufDescTable[i].Clear();
            }
        }

        //free memory
        delete [] bufPool;
        delete [] bufDescTable;
        delete  hashTable;

    }

    void BufMgr::advanceClock()
    {

        clockHand = (clockHand + 1) % numBufs;

    }

    void BufMgr::allocBuf(FrameId & frame)
    {

        //first see if all pinned
        int pinned = 0;
        for(int i = 0; i < numBufs; i++){
            if(bufDescTable[i].pinCnt > 0){
                pinned++;
            }
        }

        if(pinned >= numBufs){
            throw  BufferExceededException();
        }

        //meat of algo
        advanceClock();

        //fields
        bool found = false;

        while(!found){

            //check valid
            if(!bufDescTable[clockHand].valid){
                found = true;

            }else {

                if (bufDescTable[clockHand].refbit) {
                    bufDescTable[clockHand].refbit = false;
                    advanceClock();
                } else {

                    if (bufDescTable[clockHand].pinCnt > 0) {
                        advanceClock();
                    } else {

                        //evict and return
                        found = true;

                        //check to see if have to write back
                        if(bufDescTable[clockHand].dirty){
                            flushFile(bufDescTable[clockHand].file);
                        }
                    }

                }
            }
        }


        //clear frame
        bufDescTable[clockHand].Clear();

        //return open frame
        frame = clockHand;

    }


    void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
    {

        //check to see if page is in buffer pool
        FrameId frameNumber;

        //found in bool at entry
        if( hashTable->lookup(file, pageNo, frameNumber)){


            bufDescTable[frameNumber].refbit = true;
            bufDescTable[frameNumber].pinCnt++;

        }

            //not in pool
        else{


            //find space in pool
            allocBuf(frameNumber);

            //reps the page you are trying to read
            Page temp = file->readPage(pageNo);

            //read page into buffer pool and hash table
            bufPool[frameNumber] = temp;
            bufDescTable[frameNumber].Set(file, pageNo );
            hashTable->insert(file, pageNo, frameNumber);

        }

        //return pointer to frame that contains the page
        page = &bufPool[frameNumber];
    }


    void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
    {

        FrameId frameNumber;


        if(hashTable->lookup(file, pageNo, frameNumber)){

            if( bufDescTable[frameNumber].pinCnt == 0){
                throw PageNotPinnedException(file->filename(), pageNo, frameNumber);
            }else{
                bufDescTable[frameNumber].pinCnt--;

                if(dirty == true){
                    bufDescTable[frameNumber].dirty = true;
                }
            }
        }
    }

    void BufMgr::flushFile( const File* file)    {


        //scan for pages that belong to the file
        for (int i = 0; i < numBufs; i++) {
            if(bufDescTable[i].file  == file){

                if(bufDescTable[i].pinCnt > 0){
                    throw  PagePinnedException(file->filename(),bufDescTable[i].pageNo,  bufDescTable->frameNo);
                }

                if(!bufDescTable[i].valid ){
                    bufDescTable[i].Clear();
                    throw  BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
                }


                if(bufDescTable[i].dirty){

                    //write back and remove from hash
                    bufDescTable[i].file->writePage(bufPool[i]);
                    bufDescTable[i].dirty = false;


                }

                hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
                bufDescTable[i].Clear();


            }
        }


    }

    void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
    {

        //allocate new page
        Page newPage =  file->allocatePage();
        pageNo = newPage.page_number();

        //find where to put it
        FrameId frameNumber;
        allocBuf(frameNumber);

        //set up descriptiom table
        hashTable->insert(file, pageNo, frameNumber);
        bufDescTable[frameNumber].Set(file, pageNo);

        //actually insert it! and return it via page
        bufPool[frameNumber] = newPage;
        page =  &bufPool[frameNumber];


    }

    void BufMgr::disposePage(File* file, const PageId PageNo)
    {

        //See if page is already in pool
        FrameId frameNumber;

        if(hashTable->lookup(file, PageNo, frameNumber)){

            //free frame
            bufDescTable[frameNumber].Clear();
            hashTable->remove(file, PageNo);
        }


        file->deletePage(PageNo);

    }

    void BufMgr::printSelf(void)
    {
        BufDesc* tmpbuf;
        int validFrames = 0;

        for (std::uint32_t i = 0; i < numBufs; i++)
        {
            tmpbuf = &(bufDescTable[i]);
            std::cout << "FrameNo:" << i << " ";
            tmpbuf->Print();

            if (tmpbuf->valid == true)
                validFrames++;
        }

        std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
    }

}
