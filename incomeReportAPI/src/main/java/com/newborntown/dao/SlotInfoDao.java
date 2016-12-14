package com.newborntown.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.newborntown.utils.MongoUtil;

public interface SlotInfoDao {
  
     //自有publisherid包含的slotid列表
     public HashMap<String, ArrayList<String>> getPublisherSlotMapping(MongoUtil mongoUtil);
     

}
