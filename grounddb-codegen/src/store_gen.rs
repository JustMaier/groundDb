use grounddb::schema::SchemaDefinition;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::type_utils::{
    collection_method_name, collection_struct_name, view_params_name, view_row_name,
};

/// Generate the StoreExt trait with typed collection accessors and view methods.
pub fn generate_store_ext(schema: &SchemaDefinition) -> TokenStream {
    let mut trait_methods = Vec::new();
    let mut impl_methods = Vec::new();

    // Sort collections for deterministic output
    let mut collections: Vec<_> = schema.collections.iter().collect();
    collections.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (collection_name, _collection_def) in &collections {
        let method_name = collection_method_name(collection_name);
        let method_ident = format_ident!("{}", method_name);
        let struct_name = collection_struct_name(collection_name);
        let struct_ident = format_ident!("{}", struct_name);
        let collection_name_lit = *collection_name;

        trait_methods.push(quote! {
            fn #method_ident(&self) -> TypedCollection<#struct_ident>;
        });

        impl_methods.push(quote! {
            fn #method_ident(&self) -> TypedCollection<#struct_ident> {
                TypedCollection::new(self.store(), #collection_name_lit)
            }
        });
    }

    // Sort views for deterministic output
    let mut views: Vec<_> = schema.views.iter().collect();
    views.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (view_name, view_def) in &views {
        let method_ident = format_ident!("{}", view_name);
        let row_struct = format_ident!("{}", view_row_name(view_name));

        if view_def.params.is_some() {
            // Parameterized view
            let params_struct = format_ident!("{}", view_params_name(view_name));
            let view_name_lit = *view_name;

            trait_methods.push(quote! {
                fn #method_ident(&self, params: #params_struct) -> grounddb::Result<Vec<#row_struct>>;
            });

            impl_methods.push(quote! {
                fn #method_ident(&self, params: #params_struct) -> grounddb::Result<Vec<#row_struct>> {
                    self.store().query_view(#view_name_lit, &params)
                }
            });
        } else {
            // Static view
            let view_name_lit = *view_name;

            trait_methods.push(quote! {
                fn #method_ident(&self) -> grounddb::Result<Vec<#row_struct>>;
            });

            impl_methods.push(quote! {
                fn #method_ident(&self) -> grounddb::Result<Vec<#row_struct>> {
                    self.store().read_view(#view_name_lit)
                }
            });
        }
    }

    quote! {
        /// A typed wrapper around a grounddb collection.
        pub struct TypedCollection<T> {
            store: *const grounddb::Store,
            collection_name: &'static str,
            _phantom: std::marker::PhantomData<T>,
        }

        impl<T> TypedCollection<T> {
            fn new(store: &grounddb::Store, collection_name: &'static str) -> Self {
                Self {
                    store: store as *const grounddb::Store,
                    collection_name,
                    _phantom: std::marker::PhantomData,
                }
            }
        }

        impl<T> TypedCollection<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Clone,
        {
            pub fn get(&self, id: &str) -> grounddb::Result<grounddb::Document<T>> {
                let store = unsafe { &*self.store };
                store.get_document(self.collection_name, id)
            }

            pub fn list(&self) -> grounddb::Result<Vec<grounddb::Document<T>>> {
                let store = unsafe { &*self.store };
                store.list_documents(self.collection_name)
            }

            pub fn insert(&self, data: T, content: Option<&str>) -> grounddb::Result<String> {
                let store = unsafe { &*self.store };
                store.insert_document(self.collection_name, &data, content)
            }

            pub fn update(&self, id: &str, data: T) -> grounddb::Result<()> {
                let store = unsafe { &*self.store };
                store.update_document(self.collection_name, id, &data)
            }

            pub fn delete(&self, id: &str) -> grounddb::Result<()> {
                let store = unsafe { &*self.store };
                store.delete_document(self.collection_name, id)
            }
        }

        /// Extension trait providing typed collection and view accessors.
        pub trait StoreExt {
            fn store(&self) -> &grounddb::Store;

            #(#trait_methods)*
        }

        impl StoreExt for grounddb::Store {
            fn store(&self) -> &grounddb::Store {
                self
            }

            #(#impl_methods)*
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grounddb::schema::{SchemaDefinition, CollectionDefinition};
    use std::collections::HashMap;

    #[test]
    fn test_generate_store_ext_basic() {
        let mut collections = HashMap::new();
        collections.insert(
            "users".to_string(),
            CollectionDefinition {
                path: "users/{name}.md".to_string(),
                fields: HashMap::new(),
                content: false,
                additional_properties: false,
                strict: true,
                readonly: false,
                on_delete: None,
                id: None,
                records: None,
            },
        );

        let schema = SchemaDefinition {
            types: HashMap::new(),
            collections,
            views: HashMap::new(),
        };

        let tokens = generate_store_ext(&schema);
        let code = tokens.to_string();

        assert!(code.contains("StoreExt"));
        assert!(code.contains("fn users"));
        assert!(code.contains("TypedCollection"));
    }
}
