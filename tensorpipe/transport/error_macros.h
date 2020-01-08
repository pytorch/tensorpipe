#pragma once

#include <tensorpipe/transport/error.h>

#define TP_CREATE_ERROR(typ, ...) (Error(std::make_shared<typ>(__VA_ARGS__)))
