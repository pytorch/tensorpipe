# Result variables:
# TP_CUDA_LINK_LIBRARIES is list of dependent libraries to be linked
# TP_CUDA_INCLUDE_DIRS is list of include paths to be used

# find_package will respect <Package>_ROOT variables
if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.12.0)
  cmake_policy(SET CMP0074 NEW)
endif()

# Try to find the newer CUDAToolkit, otherwise use the deprecated find_package(CUDA)
find_package(CUDAToolkit)
if(CUDAToolkit_FOUND)
  if(CUDA_USE_STATIC_CUDA_RUNTIME)
    set(TP_CUDA_LINK_LIBRARIES CUDA::cudart_static)
  else()
    set(TP_CUDA_LINK_LIBRARIES CUDA::cudart)
  endif()
  set(TP_CUDA_INCLUDE_DIRS "${CUDAToolkit_INCLUDE_DIR}")
else()
  find_package(CUDA REQUIRED)

  set(TP_CUDA_LINK_LIBRARIES ${CUDA_LIBRARIES})
  set(TP_CUDA_INCLUDE_DIRS ${CUDA_INCLUDE_DIRS})
endif()
