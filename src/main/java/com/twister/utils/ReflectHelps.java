package com.twister.utils;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utilities for reflection
 * 
 * 
 */
public class ReflectHelps {
	private static final ClassLoader defaultLoader = ReflectHelps.class.getClassLoader();
	private static final String[] CGLIB_PACKAGES = { "java.lang", };
	private static final Map primitives = new HashMap(8);
	private static final Map transforms = new HashMap(8);
	static {
		primitives.put("byte", Byte.TYPE);
		primitives.put("char", Character.TYPE);
		primitives.put("double", Double.TYPE);
		primitives.put("float", Float.TYPE);
		primitives.put("int", Integer.TYPE);
		primitives.put("long", Long.TYPE);
		primitives.put("short", Short.TYPE);
		primitives.put("boolean", Boolean.TYPE);
		
		transforms.put("byte", "B");
		transforms.put("char", "C");
		transforms.put("double", "D");
		transforms.put("float", "F");
		transforms.put("int", "I");
		transforms.put("long", "J");
		transforms.put("short", "S");
		transforms.put("boolean", "Z");
	}
	
	private ReflectHelps() { }
	/**
	 * @param targetType
	 *            The type to check
	 * @param fieldName
	 *            The field name to check for
	 * @return true if the field is found
	 */
	public static boolean hasField(Class<?> targetType, String fieldName) {
		try {
			targetType.getDeclaredField(fieldName);			
		} catch (NoSuchFieldException e) {
			return false;
		}
		return true;
	}
 
	
	public static PropertyDescriptor[] getBeanProperties(Class<?> type) {
		return getPropertiesHelper(type, true, true);
	}
	
	public static PropertyDescriptor[] getBeanGetters(Class<?> type) {
		return getPropertiesHelper(type, true, false);
	}
	
	public static PropertyDescriptor[] getBeanSetters(Class<?> type) {
		return getPropertiesHelper(type, false, true);
	}
	
	private static PropertyDescriptor[] getPropertiesHelper(Class<?> type, boolean read, boolean write) {
		try {
			BeanInfo info = Introspector.getBeanInfo(type, Object.class);
			PropertyDescriptor[] all = info.getPropertyDescriptors();
			if (read && write) {
				return all;
			}
			List properties = new ArrayList(all.length);
			for (int i = 0; i < all.length; i++) {
				PropertyDescriptor pd = all[i];
				if ((read && pd.getReadMethod() != null) || (write && pd.getWriteMethod() != null)) {
					properties.add(pd);
				}
			}
			return (PropertyDescriptor[]) properties.toArray(new PropertyDescriptor[properties.size()]);
		} catch (IntrospectionException e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	public static Method[] findInterfaceMethods(Class<?> iface) {
		if (!iface.isInterface()) {
			throw new IllegalArgumentException(iface + " is not an interface");
		}
		Method[] methods = iface.getDeclaredMethods();		 
		return methods;
	}
	/**
	 *  
	 * 
	 * @param className.cls
	 * @param methodname
	 *           
	 * @return The Method
	 */
	public static Method findInterfaceMethod(Class<?> iface,String methodName) {
		if (!iface.isInterface()) {
			throw new IllegalArgumentException(iface + " is not an interface");
		}
		Method method=null;
		Method[] methods = iface.getDeclaredMethods();
		System.out.println(methods.length);
		for(int i=0;i<methods.length;i++){
			if (methods[i].getName().equals(methodName)){
				method=methods[i];
				break;
			}			 
		}	 
		return method;
	}
	
	/**
	 * Load the given class using the default constructor
	 * 
	 * @param className
	 *            The name of the class package.fullname
	 * @return The class object
	 */
	public static Class<?> loadClass(String className) {
		try {
			return Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException(e);
		}
	}  
	 
	/**
	 * Load the given class using a specific class loader.
	 * 
	 * @param className
	 *            The name of the class
	 * @param cl
	 *            The Class Loader to be used for finding the class.
	 * @return The class object
	 */
	public static Class<?> loadClass(String className, ClassLoader cl) {
		try {
			return Class.forName(className, false, cl);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	/**
	 * Call the no-arg constructor for the given class
	 * 
	 * @param <T>
	 *            The type of the thing to construct
	 * @param klass
	 *            The class
	 * @return The constructed thing
	 */
	public static <T> T callConstructor(Class<T> klass) {
		return callConstructor(klass, new Class<?>[0], new Object[0]);
	}
	
	/**
	 * Call the constructor for the given class, inferring the correct types for
	 * the arguments. This could be confusing if there are multiple constructors
	 * with the same number of arguments and the values themselves don't
	 * disambiguate.
	 * 
	 * @param klass
	 *            The class to construct
	 * @param args
	 *            The arguments
	 * @return The constructed value
	 */
	public static <T> T callConstructor(Class<T> klass, Object[] args) {
		Class<?>[] klasses = new Class[args.length];
		for (int i = 0; i < args.length; i++)
			klasses[i] = args[i].getClass();
		return callConstructor(klass, klasses, args);
	}
	
	/**
	 * Call the class constructor with the given arguments
	 * 
	 * @param c
	 *            The class
	 * @param args
	 *            The arguments type
	 * @param args
	 *            The arguments
	 * @return The constructed object
	 */
	public static <T> T callConstructor(Class<T> c, Class<?>[] argTypes, Object[] args) {
		try {
			Constructor<T> cons = c.getConstructor(argTypes);
			return cons.newInstance(args);
		} catch (InvocationTargetException e) {
			throw getCause(e);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException(e);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(e);
		} catch (InstantiationException e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	/**
	 * Call the named method
	 * 
	 * @param obj
	 *            The object to call the method on
	 * @param c
	 *            The class of the object
	 * @param name
	 *            The name of the method
	 * @param args
	 *            The method arguments
	 * @return The result of the method
	 */
	public static <T> Object callMethod(Object obj, Class<T> c, String name, Class<?>[] classes, Object[] args) {
		try {
			Method m = getMethod(c, name, classes);
			return m.invoke(obj, args);
		} catch (InvocationTargetException e) {
			throw getCause(e);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException(e);
		}
	}
	
	/**
	 * Get the named method from the class
	 * 
	 * @param c
	 *            The class to get the method from
	 * @param name
	 *            The method name
	 * @param argTypes
	 *            The argument types
	 * @return The method ,etc String.class
	 */
	public static <T> Method getMethod(Class<T> c, String name, Class<?>... argTypes) {
		try {
			return c.getMethod(name, argTypes);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(e);
		}
	}
	 
	
	/**
	 * Get the root cause of the Exception
	 * 
	 * @param e
	 *            The Exception
	 * @return The root cause of the Exception
	 */
	private static RuntimeException getCause(InvocationTargetException e) {
		Throwable cause = e.getCause();
		if (cause instanceof RuntimeException)
			throw (RuntimeException) cause;
		else
			throw new IllegalArgumentException(e.getCause());
	}
	
}