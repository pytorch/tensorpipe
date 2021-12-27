/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <iostream>
#include <regex>
#include <unordered_set>

#include <clang/ASTMatchers/ASTMatchFinder.h>
#include <clang/ASTMatchers/ASTMatchers.h>
#include <clang/Frontend/FrontendActions.h>
#include <clang/Tooling/CommonOptionsParser.h>
#include <clang/Tooling/Tooling.h>
#include <llvm/Support/CommandLine.h>

using namespace clang::ast_matchers;
using namespace clang::tooling;
using namespace llvm;

namespace {

std::string exprToString(const clang::Expr& e) {
  std::string statement;
  raw_string_ostream stream(statement);
  e.printPretty(stream, nullptr, clang::PrintingPolicy(clang::LangOptions()));
  stream.flush();

  return statement;
}

std::string cleanUp(const std::string& s) {
  std::string res = s;
  res = std::regex_replace(res, std::regex("(struct|class) [a-zA-Z_]+::"), "");
  res = std::regex_replace(res, std::regex("this->"), "");
  return res;
}

std::string escape(const std::string& s) {
  std::string res = s;
  res = std::regex_replace(res, std::regex("\\{"), "\\{");
  res = std::regex_replace(res, std::regex("\\}"), "\\}");
  res = std::regex_replace(res, std::regex(">"), "\\>");
  res = std::regex_replace(res, std::regex("<"), "\\<");
  res = std::regex_replace(res, std::regex("\\|"), "\\|");
  return res;
}

class MethodPrinter : public MatchFinder::MatchCallback {
  std::unordered_set<std::string> nodes_;

  void addNode(const std::string& label) {
    std::cout << label << " [label=<<b>" << label
              << "</b>>,group=states,fontstyle=\"bold\"];" << std::endl;
    nodes_.insert(label);
  }

 public:
  void run(const MatchFinder::MatchResult& result) override {
    static int edgeCount = 0;

    const clang::CallExpr& e = *result.Nodes.getNodeAs<clang::CallExpr>("x");
    std::string edgeId = "edge" + std::to_string(edgeCount++);
    std::string fromId = cleanUp(exprToString(*e.getArg(1)));
    std::string toId = cleanUp(exprToString(*e.getArg(2)));

    if (nodes_.count(fromId) == 0) {
      addNode(fromId);
    }

    if (nodes_.count(toId) == 0) {
      addNode(toId);
    }

    std::string edgeColor = "orange3";
    int edgeWeight = 100;
    std::string cond = cleanUp(exprToString(*e.getArg(3)));
    if (std::regex_search(cond, std::regex("^error_"))) {
      edgeColor = "red3";
      edgeWeight = 0;
    }
    if (std::regex_search(cond, std::regex("^!error_"))) {
      edgeColor = "forestgreen";
    }
    cond = std::regex_replace(cond, std::regex(" \\&\\&"), "\\n");
    cond = escape(cond);

    std::string actions = cleanUp(exprToString(*e.getArg(4)));
    actions = std::regex_replace(actions, std::regex("(\\{|\\})"), "");
    actions = std::regex_replace(actions, std::regex(", "), "\\n");
    actions = std::regex_replace(actions, std::regex("\\&"), "");

    std::cout << edgeId << " [label=\"{" << cond << "|" << actions
              << "}\",shape=record,style=\"rounded,dashed\",color=\""
              << edgeColor << "\"];" << std::endl;

    std::cout << fromId << " -> " << edgeId << "[dir=\"none\",color=\""
              << edgeColor << "\",style=\"dashed\",weight=" << edgeWeight
              << "];" << std::endl;

    std::cout << edgeId << " -> " << toId << "[color=\"" << edgeColor
              << "\",style=\"dashed\",weight=" << edgeWeight << "];"
              << std::endl;
  }
};

} // namespace

int main(int argc, const char* argv[]) {
  cl::OptionCategory category("dump_state_machine");
  cl::opt<std::string> methodName(
      "method",
      cl::Required,
      cl::cat(category),
      cl::desc(
          "Name of the method implementing the state machine's transitions."),
      cl::value_desc("method_name"));

  CommonOptionsParser optionsParser(argc, argv, category, cl::Required);
  ClangTool tool(
      optionsParser.getCompilations(), optionsParser.getSourcePathList());
  auto methodMatcher = callExpr(
                           callee(cxxMethodDecl(hasName("attemptTransition"))),
                           hasAncestor(cxxMethodDecl(hasName(methodName))))
                           .bind("x");
  MethodPrinter printer;
  MatchFinder finder;
  finder.addMatcher(methodMatcher, &printer);
  std::cout << "digraph {" << std::endl
            << "graph [rankdir=TB]" << std::endl
            << "node [shape=box]" << std::endl;
  int res = tool.run(newFrontendActionFactory(&finder).get());
  std::cout << "}" << std::endl;
  return res;
}
