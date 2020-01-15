#pragma once

#include <tensorpipe/core/error.h>

#define TP_CREATE_ERROR(typ, ...) (Error(std::make_shared<typ>(__VA_ARGS__)))
