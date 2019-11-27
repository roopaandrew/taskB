package com.assessment.taskB;

import org.springframework.batch.item.ItemProcessor;

public class DataProcessor implements ItemProcessor<Employee, String>{
	
	public static final int CIPHER_SHIFT = 4;

	@Override
	public String process(Employee employee) throws Exception {
		
		StringBuffer transformedItem = encrypt(employee.getName()+"."+employee.getPlace(), CIPHER_SHIFT);
		
		return transformedItem.toString();
	}
	
	// Encrypts text using a shift of s 
    public StringBuffer encrypt(String text, int s) 
    { 
        StringBuffer result= new StringBuffer(); 
  
        for (int i=0; i<text.length(); i++) 
        { 
            if (Character.isUpperCase(text.charAt(i))) 
            { 
                char ch = (char)(((int)text.charAt(i) + 
                                  s - 65) % 26 + 65); 
                result.append(ch); 
            } 
            else
            { 
                char ch = (char)(((int)text.charAt(i) + 
                                  s - 97) % 26 + 97); 
                result.append(ch); 
            } 
        } 
        return result; 
    } 

}
