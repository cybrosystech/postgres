ALTER TABLE sale_order_transaction_rel ADD CONSTRAINT sale_order_transaction_rel_sale_order_id_fkey FOREIGN KEY (sale_order_id) REFERENCES sale_order(id) ON DELETE CASCADE;
ALTER TABLE sale_order_tag_rel ADD CONSTRAINT sale_order_tag_rel_order_id_fkey FOREIGN KEY (order_id) REFERENCES sale_order(id) ON DELETE CASCADE;
ALTER TABLE sale_order_line ADD CONSTRAINT sale_order_line_order_id_fkey FOREIGN KEY (order_id) REFERENCES sale_order(id) ON DELETE CASCADE;
ALTER TABLE sale_order_mass_cancel_wizard_rel ADD CONSTRAINT sale_order_mass_cancel_wizard_rel_sale_order_id_fkey FOREIGN KEY (sale_order_id) REFERENCES sale_order(id) ON DELETE CASCADE;
ALTER TABLE sale_advance_payment_inv_sale_order_rel ADD CONSTRAINT sale_advance_payment_inv_sale_order_rel_sale_order_id_fkey FOREIGN KEY (sale_order_id) REFERENCES sale_order(id) ON DELETE CASCADE;
ALTER TABLE sale_order_discount ADD CONSTRAINT sale_order_discount_sale_order_id_fkey FOREIGN KEY (sale_order_id) REFERENCES sale_order(id) ON DELETE CASCADE;
ALTER TABLE quotation_document_sale_order_rel ADD CONSTRAINT quotation_document_sale_order_rel_sale_order_id_fkey FOREIGN KEY (sale_order_id) REFERENCES sale_order(id) ON DELETE CASCADE;
