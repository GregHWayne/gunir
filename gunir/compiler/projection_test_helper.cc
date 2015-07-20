// Copyright (C) 2015. The Gunir Authors. All rights reserved.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "gunir/compiler/compiler_test_helper.h"

namespace gunir {
namespace compiler {

/*
 * DocumentFilter generate result for the following query
 * SELECT
 * docid, name.url, name.language.code, name.language.country
 * FROM Document
 * WHERE
 * docid % 2 == 0 AND
 * (name.url CONTAINS 'C' OR name.url CONTAINS 'D') AND
 * LENGTH(name.language.country) > 3
 */
::google::protobuf::Message* DocumentFilter(const PBMessage* input) {
    const Document* pdoc = dynamic_cast<const Document*>(input); // NOLINT
    const Document& input_doc = *pdoc;

    if (input_doc.docid() % 2 != 0) {
        return NULL;
    }

    if (input_doc.name_size() == 0) {
        return NULL;
    }

    Document doc;
    doc.set_docid(input_doc.docid());

    for (int iname = 0; iname < input_doc.name_size(); ++iname) {
        const Name& name = input_doc.name(iname);
        Name new_name;

        if (!name.has_url()) {
            continue;
        }

        if (name.url().find("C") == std::string::npos &&
            name.url().find("D") == std::string::npos) {
            continue;
        }
        new_name.set_url(name.url());

        if (name.language_size() == 0) {
            continue;
        }

        for (int ilang = 0; ilang < name.language_size(); ++ilang) {
            const Language& lang = name.language(ilang);
            if (!lang.has_country() || lang.country().size() <= 3) {
                continue;
            }

            *(new_name.add_language()) = lang;
        }

        if (new_name.language_size() > 0) {
            *(doc.add_name()) = new_name;
        }
    }

    if (doc.name_size() == 0) {
        return NULL;
    }
    return new Document(doc);
}

void CreateProjectionTestData() {
    CreateTestData("projection_test",
                   "./testdata/Document.proto",
                   "Document",
                   "./testdata/Document.proto",
                   "Document",
                   RandDocument,
                   DocumentFilter);
}

} // namespace compiler
} // namespace gunir

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, false);

    ::gunir::compiler::CreateProjectionTestData();
    return 0;
}
