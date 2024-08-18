#include "Algebra.h"
#include <iostream>
#include <cstring>
#include <stdlib.h>

/* used to select all the records that satisfy a condition.
the arguments of the function are
- srcRel - the source relation we want to select from
- targetRel - the relation we want to select into. (ignore for now)
- attr - the attribute that the condition is checking
- op - the operator of the condition
- strVal - the value that we want to compare against (represented as a string)
*/

// will return if a string can be parsed as a floating point number
bool isNumber(char *str){

    int len;
    float ignore;

    /*
        sscanf returns the number of elements read, so if there is no float matching
        the first %f, ret will be 0, else it'll be 1

        %n gets the number of characters read. this scanf sequence will read the
        first float ignoring all the whitespace before and after. and the number of
        characters read that far will be stored in len. if len == strlen(str), then
        the string only contains a float with/without whitespace. else, there's other
        characters.
    */

   int ret = sscanf(str,"%f %n",&ignore, &len);

   return ret == 1 && len == strlen(str);

}

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]){
    int srcRelId = OpenRelTable::getRelId(srcRel);

    
    if(srcRelId == E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }

    AttrCatEntry attrCatEntry;

    int ret = AttrCacheTable::getAttrCatEntry(srcRelId,attr,&attrCatEntry);

    std::cout<<"ret: "<<ret<<std::endl;
    if(ret != SUCCESS){
        return ret;
    }

    /*** Convert strVal (string) to an attribute of data type NUMBER or STRING ***/

    int type = attrCatEntry.attrType;

    Attribute attrVal;

    if(type == NUMBER){   // the isNumber() function is implemented below
        if(isNumber(strVal)){
            attrVal.nVal = atof(strVal);
        }else{
            return E_ATTRTYPEMISMATCH;
        }
    }else if(type == STRING){
        strcpy(attrVal.sVal,strVal);
    }

    /*** Selecting records from the source relation ***/

    // Before calling the search function, reset the search to start from the first hit
    // using RelCacheTable::resetSearchIndex()

    RelCatEntry relCatEntry;

    // get relCatEntry using RelCacheTable::getRelCatEntry()

    RelCacheTable::getRelCatEntry(srcRelId,&relCatEntry);

    /************************
     The following code prints the contents of a relation directly to the output
    console. Direct console output is not permitted by the actual the NITCbase
    specification and the output can only be inserted into a new relation. We will
    be modifying it in the later stages to match the specification.
    ************************/
    std::cout<<"|";

    for(int i=0;i<relCatEntry.numAttrs;i++){
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId,i,&attrCatEntry);

        std::cout<<" "<<attrCatEntry.attrName<<" |";
    }
    std::cout<<std::endl;

    while(true){
        RecId searchRes = BlockAccess::linearSearch(srcRelId,attr,attrVal,op);

        if(searchRes.block != -1 && searchRes.slot != -1){
            
            Attribute record[relCatEntry.numAttrs];
            RecBuffer recBuf(searchRes.block);

            recBuf.getRecord(record,searchRes.slot);

            //printing the values---->

            std::cout<<"|";

            for(int i=0;i<relCatEntry.numAttrs;i++){
                AttrCatEntry attrCatEntry;
                AttrCacheTable::getAttrCatEntry(srcRelId,i,&attrCatEntry);
                
                if(attrCatEntry.attrType == NUMBER){
                    std::cout<<" "<<record[i].nVal<<" |";
                }else{
                    std::cout<<" "<<record[i].sVal<<" |";
                }
            }
            std::cout<<std::endl;

            //<---done printing the value
            
        }else{
            //no records that satisy the condition anymore
            break;
        }
    }

    return SUCCESS;
   
}
