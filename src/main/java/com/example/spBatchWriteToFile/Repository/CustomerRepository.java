package com.example.spBatchWriteToFile.Repository;

import com.example.spBatchWriteToFile.Models.Customer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CustomerRepository extends JpaRepository<Customer,Integer> {
}
