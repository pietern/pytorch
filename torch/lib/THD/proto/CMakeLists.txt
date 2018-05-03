add_library(Store
  Store.cpp
  FileStore.cpp
  )

add_executable(FileStoreTest
  FileStoreTest.cpp
  )

target_link_libraries(FileStoreTest PUBLIC Store pthread)
