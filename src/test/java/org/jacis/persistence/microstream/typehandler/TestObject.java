package org.jacis.persistence.microstream.typehandler;

public class TestObject {

  private final String firstName;
  private final String lastName;
  private int age;

  public TestObject(String firstName, String lastName) {
    this.firstName = firstName;
    this.lastName = lastName;
    age = 18;
  }

  public String getFirstName() {
    return firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public int getAge() {
    return age;
  }

  public TestObject setAge(int age) {
    this.age = age;
    return this;
  }

  @Override
  public String toString() {
    return firstName + " " + lastName + " (" + age + ")";
  }

}
